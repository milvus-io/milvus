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

#define OLD_M	 4 + STACK(%esi)
#define OLD_N	 8 + STACK(%esi)
#define OLD_K	12 + STACK(%esi)
#define OLD_ALPHA_R	16 + STACK(%esi)
#define OLD_ALPHA_I	20 + STACK(%esi)
#define OLD_A	24 + STACK(%esi)
#define OLD_B	28 + STACK(%esi)
#define OLD_C	32 + STACK(%esi)
#define OLD_LDC	36 + STACK(%esi)

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

#if defined(PENRYN) || defined(DUNNINGTON)
#define PREFETCH     prefetcht0
#define PREFETCHSIZE   96
#endif

#ifdef PENTIUM4
#define PREFETCH     prefetcht0
#define PREFETCHSIZE   96
#endif

#ifdef PENTIUMM
#define PREFETCH     prefetcht0
#define PREFETCHSIZE   96
#endif

#define AA	%edx
#define	BB	%ecx
#define LDC	%ebp

#define KERNEL1(address) \
	mulps	%xmm0, %xmm2; \
	PREFETCH  (PREFETCHSIZE +  0) * SIZE + (address) * SIZE(AA); \
	addps	%xmm2, %xmm4; \
	movshdup  0 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm5; \
	movsldup  4 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm6; \
	movshdup  4 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	movaps	  4 * SIZE + 1 * (address) * SIZE(AA), %xmm0; \
	addps	%xmm2, %xmm7; \
	movsldup  8 * SIZE + 2 * (address) * SIZE(BB), %xmm2

#define KERNEL2(address) \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm4; \
	movshdup  8 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm5; \
	movsldup 12 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm6; \
	movshdup 12 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	movaps	  8 * SIZE + 1 * (address) * SIZE(AA), %xmm0; \
	addps	%xmm2, %xmm7; \
	movsldup 32 * SIZE + 2 * (address) * SIZE(BB), %xmm2

#define KERNEL3(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movshdup 16 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movsldup 20 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm6; \
	movshdup 20 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	movaps	 12 * SIZE + 1 * (address) * SIZE(AA), %xmm0; \
	addps	%xmm3, %xmm7; \
	movsldup 24 * SIZE + 2 * (address) * SIZE(BB), %xmm3

#define KERNEL4(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movshdup 24 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movsldup 28 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm6; \
	movshdup 28 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	movaps	 32 * SIZE + 1 * (address) * SIZE(AA), %xmm0; \
	addps	%xmm3, %xmm7; \
	movsldup 48 * SIZE + 2 * (address) * SIZE(BB), %xmm3

#define KERNEL5(address) \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movshdup 32 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movsldup 36 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm6; \
	movshdup 36 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	movaps	 20 * SIZE + 1 * (address) * SIZE(AA), %xmm1; \
	addps	%xmm2, %xmm7

#define KERNEL6(address) \
	movsldup 40 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movshdup 40 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movsldup 44 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm6; \
	movshdup 44 * SIZE + 2 * (address) * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	movaps	 24 * SIZE + 1 * (address) * SIZE(AA), %xmm1; \
	addps	%xmm2, %xmm7; \
	movsldup 64 * SIZE + 2 * (address) * SIZE(BB), %xmm2

#define KERNEL7(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movshdup 48 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movsldup 52 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm6; \
	movshdup 52 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	movaps	 28 * SIZE + 1 * (address) * SIZE(AA), %xmm1; \
	addps	%xmm3, %xmm7; \
	movsldup 56 * SIZE + 2 * (address) * SIZE(BB), %xmm3

#define KERNEL8(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movshdup 56 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movsldup 60 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm6; \
	movshdup 60 * SIZE + 2 * (address) * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	movaps	 48 * SIZE + 1 * (address) * SIZE(AA), %xmm1; \
	addps	%xmm3, %xmm7; \
	movsldup 80 * SIZE + 2 * (address) * SIZE(BB), %xmm3


	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	%esp, %esi	# save old stack
	subl	$128 + LOCAL_BUFFER_SIZE, %esp
	movl	OLD_M, %ebx
	andl	$-1024, %esp	# align stack

	STACK_TOUCHING

	movl	OLD_N, %eax
	movl	OLD_K, %ecx
	movl	OLD_A, %edx
	movss	OLD_ALPHA_R,  %xmm0
	movss	OLD_ALPHA_I,  %xmm1

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	movl	%edx, A
	movl	%esi, OLD_STACK

	movl	OLD_B, %edi
	movl	OLD_C, %ebx

	unpcklps %xmm1, %xmm0
	movlhps	 %xmm0, %xmm0

	movaps	 %xmm0, ALPHA

	movl	%ebx, C
	movl	OLD_LDC, LDC
#ifdef TRMMKERNEL
	movss	%xmm4, OFFSET
	movss	%xmm4, KK
#ifndef LEFT
	negl	KK
#endif
#endif

	sall	$ZBASE_SHIFT, LDC

	sarl	$2, %eax
	movl	%eax, J
	jle	.L40

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

/* Copying to Sub Buffer */
	leal	BUFFER, %ecx

	movl	K, %eax
	sarl	$2, %eax
	jle	.L05
	ALIGN_4

.L02:
	movddup	 0 * SIZE(%edi), %xmm0
	movddup	 2 * SIZE(%edi), %xmm1
	movddup	 4 * SIZE(%edi), %xmm2
	movddup	 6 * SIZE(%edi), %xmm3
	movddup	 8 * SIZE(%edi), %xmm4
	movddup	10 * SIZE(%edi), %xmm5
	movddup	12 * SIZE(%edi), %xmm6
	movddup	14 * SIZE(%edi), %xmm7

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)
	movaps	%xmm4, 16 * SIZE(%ecx)
	movaps	%xmm5, 20 * SIZE(%ecx)
	movaps	%xmm6, 24 * SIZE(%ecx)
	movaps	%xmm7, 28 * SIZE(%ecx)

#	prefetcht1	128 * SIZE(%ecx)
	prefetcht0	112 * SIZE(%edi)

	addl	$16 * SIZE, %edi
	addl	$32 * SIZE, %ecx
	decl	%eax
	jne	.L02
	ALIGN_2

.L05:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L10
	ALIGN_2

.L06:
	movddup	 0 * SIZE(%edi), %xmm0
	movddup	 2 * SIZE(%edi), %xmm1

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)

	addl	$4 * SIZE, %edi
	addl	$8 * SIZE, %ecx
	decl	%eax
	jne	.L06
	ALIGN_4

.L10:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

	movaps	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	16 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movsldup  0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movsldup 16 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

	leal	(LDC, LDC, 2), %eax

	prefetchnta	4 * SIZE(%esi)
	prefetchnta	4 * SIZE(%esi, LDC)
	prefetchnta	4 * SIZE(%esi, LDC, 2)
	prefetchnta	4 * SIZE(%esi, %eax)

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
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif

#if 1
	andl	$-8, %eax
	sall	$4, %eax
	je	.L15
.L1X:
	KERNEL1(32 *  0)
	KERNEL2(32 *  0)
	KERNEL3(32 *  0)
	KERNEL4(32 *  0)
	KERNEL5(32 *  0)
	KERNEL6(32 *  0)
	KERNEL7(32 *  0)
	KERNEL8(32 *  0)
	cmpl	$128 *  1, %eax
	jle	.L12
	KERNEL1(32 *  1)
	KERNEL2(32 *  1)
	KERNEL3(32 *  1)
	KERNEL4(32 *  1)
	KERNEL5(32 *  1)
	KERNEL6(32 *  1)
	KERNEL7(32 *  1)
	KERNEL8(32 *  1)
	cmpl	$128 *  2, %eax
	jle	.L12
	KERNEL1(32 *  2)
	KERNEL2(32 *  2)
	KERNEL3(32 *  2)
	KERNEL4(32 *  2)
	KERNEL5(32 *  2)
	KERNEL6(32 *  2)
	KERNEL7(32 *  2)
	KERNEL8(32 *  2)
	cmpl	$128 *  3, %eax
	jle	.L12
	KERNEL1(32 *  3)
	KERNEL2(32 *  3)
	KERNEL3(32 *  3)
	KERNEL4(32 *  3)
	KERNEL5(32 *  3)
	KERNEL6(32 *  3)
	KERNEL7(32 *  3)
	KERNEL8(32 *  3)
	cmpl	$128 *  4, %eax
	jle	.L12
	KERNEL1(32 *  4)
	KERNEL2(32 *  4)
	KERNEL3(32 *  4)
	KERNEL4(32 *  4)
	KERNEL5(32 *  4)
	KERNEL6(32 *  4)
	KERNEL7(32 *  4)
	KERNEL8(32 *  4)
	cmpl	$128 *  5, %eax
	jle	.L12
	KERNEL1(32 *  5)
	KERNEL2(32 *  5)
	KERNEL3(32 *  5)
	KERNEL4(32 *  5)
	KERNEL5(32 *  5)
	KERNEL6(32 *  5)
	KERNEL7(32 *  5)
	KERNEL8(32 *  5)
	cmpl	$128 *  6, %eax
	jle	.L12
	KERNEL1(32 *  6)
	KERNEL2(32 *  6)
	KERNEL3(32 *  6)
	KERNEL4(32 *  6)
	KERNEL5(32 *  6)
	KERNEL6(32 *  6)
	KERNEL7(32 *  6)
	KERNEL8(32 *  6)
	cmpl	$128 *  7, %eax
	jle	.L12
	KERNEL1(32 *  7)
	KERNEL2(32 *  7)
	KERNEL3(32 *  7)
	KERNEL4(32 *  7)
	KERNEL5(32 *  7)
	KERNEL6(32 *  7)
	KERNEL7(32 *  7)
	KERNEL8(32 *  7)
#if 1
	cmpl	$128 *  8, %eax
	jle	.L12
	KERNEL1(32 *  8)
	KERNEL2(32 *  8)
	KERNEL3(32 *  8)
	KERNEL4(32 *  8)
	KERNEL5(32 *  8)
	KERNEL6(32 *  8)
	KERNEL7(32 *  8)
	KERNEL8(32 *  8)
	cmpl	$128 *  9, %eax
	jle	.L12
	KERNEL1(32 *  9)
	KERNEL2(32 *  9)
	KERNEL3(32 *  9)
	KERNEL4(32 *  9)
	KERNEL5(32 *  9)
	KERNEL6(32 *  9)
	KERNEL7(32 *  9)
	KERNEL8(32 *  9)
	cmpl	$128 * 10, %eax
	jle	.L12
	KERNEL1(32 * 10)
	KERNEL2(32 * 10)
	KERNEL3(32 * 10)
	KERNEL4(32 * 10)
	KERNEL5(32 * 10)
	KERNEL6(32 * 10)
	KERNEL7(32 * 10)
	KERNEL8(32 * 10)
	cmpl	$128 * 11, %eax
	jle	.L12
	KERNEL1(32 * 11)
	KERNEL2(32 * 11)
	KERNEL3(32 * 11)
	KERNEL4(32 * 11)
	KERNEL5(32 * 11)
	KERNEL6(32 * 11)
	KERNEL7(32 * 11)
	KERNEL8(32 * 11)
	cmpl	$128 * 12, %eax
	jle	.L12
	KERNEL1(32 * 12)
	KERNEL2(32 * 12)
	KERNEL3(32 * 12)
	KERNEL4(32 * 12)
	KERNEL5(32 * 12)
	KERNEL6(32 * 12)
	KERNEL7(32 * 12)
	KERNEL8(32 * 12)
	cmpl	$128 * 13, %eax
	jle	.L12
	KERNEL1(32 * 13)
	KERNEL2(32 * 13)
	KERNEL3(32 * 13)
	KERNEL4(32 * 13)
	KERNEL5(32 * 13)
	KERNEL6(32 * 13)
	KERNEL7(32 * 13)
	KERNEL8(32 * 13)
	cmpl	$128 * 14, %eax
	jle	.L12
	KERNEL1(32 * 14)
	KERNEL2(32 * 14)
	KERNEL3(32 * 14)
	KERNEL4(32 * 14)
	KERNEL5(32 * 14)
	KERNEL6(32 * 14)
	KERNEL7(32 * 14)
	KERNEL8(32 * 14)
	cmpl	$128 * 15, %eax
	jle	.L12
	KERNEL1(32 * 15)
	KERNEL2(32 * 15)
	KERNEL3(32 * 15)
	KERNEL4(32 * 15)
	KERNEL5(32 * 15)
	KERNEL6(32 * 15)
	KERNEL7(32 * 15)
	KERNEL8(32 * 15)
#else
	addl	$128 * 4  * SIZE, BB
	addl	$128 * 2  * SIZE, AA
	subl	$128 * 8, %eax
	jg	.L1X
	jmp	.L15
#endif

.L12:
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
	ALIGN_4
#else
	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	KERNEL1(32 *  7)
	KERNEL2(32 *  7)
	KERNEL3(32 *  7)
	KERNEL4(32 *  7)
	KERNEL5(32 *  7)
	KERNEL6(32 *  7)
	KERNEL7(32 *  7)
	KERNEL8(32 *  7)

	addl   $32 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L12
	ALIGN_4
#endif

.L15:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_4

.L16:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movshdup  0 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movsldup  4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movshdup  4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	  4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movsldup  8 * SIZE(BB), %xmm2

	addl	$4 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	leal	(LDC, LDC, 2), %eax

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0
	movsd	 4 * SIZE(%esi), %xmm1
	movhps	 6 * SIZE(%esi), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)
	movlps	%xmm1,   4 * SIZE(%esi)
	movhps	%xmm1,   6 * SIZE(%esi)

	movsd	 0 * SIZE(%esi, LDC), %xmm0
	movhps	 2 * SIZE(%esi, LDC), %xmm0
	movsd	 4 * SIZE(%esi, LDC), %xmm1
	movhps	 6 * SIZE(%esi, LDC), %xmm1

	pshufd	$0x50, %xmm5,  %xmm2
	pshufd	$0xfa, %xmm5,  %xmm5

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm5

	addps	%xmm2, %xmm0
	addps	%xmm5, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi, LDC)
	movhps	%xmm0,   2 * SIZE(%esi, LDC)
	movlps	%xmm1,   4 * SIZE(%esi, LDC)
	movhps	%xmm1,   6 * SIZE(%esi, LDC)

	movsd	 0 * SIZE(%esi, LDC, 2), %xmm0
	movhps	 2 * SIZE(%esi, LDC, 2), %xmm0
	movsd	 4 * SIZE(%esi, LDC, 2), %xmm1
	movhps	 6 * SIZE(%esi, LDC, 2), %xmm1

	pshufd	$0x50, %xmm6,  %xmm2
	pshufd	$0xfa, %xmm6,  %xmm6

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm6

	addps	%xmm2, %xmm0
	addps	%xmm6, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi, LDC, 2)
	movhps	%xmm0,   2 * SIZE(%esi, LDC, 2)
	movlps	%xmm1,   4 * SIZE(%esi, LDC, 2)
	movhps	%xmm1,   6 * SIZE(%esi, LDC, 2)

	movsd	 0 * SIZE(%esi, %eax), %xmm0
	movhps	 2 * SIZE(%esi, %eax), %xmm0
	movsd	 4 * SIZE(%esi, %eax), %xmm1
	movhps	 6 * SIZE(%esi, %eax), %xmm1

	pshufd	$0x50, %xmm7,  %xmm2
	pshufd	$0xfa, %xmm7,  %xmm7

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm7

	addps	%xmm2, %xmm0
	addps	%xmm7, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi, %eax)
	movhps	%xmm0,   2 * SIZE(%esi, %eax)
	movlps	%xmm1,   4 * SIZE(%esi, %eax)
	movhps	%xmm1,   6 * SIZE(%esi, %eax)

	addl	$8 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L11
	ALIGN_4

.L20:
	testl	$2, M
	je	.L30

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

	movddup	  0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movddup	  8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movsd     0 * SIZE(BB), %xmm2
	movsd    16 * SIZE(BB), %xmm3

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
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L25
	ALIGN_4

.L22:
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AA)
	addps	%xmm2, %xmm4
	movsd     4 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	movddup	  2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsd     8 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movsd    12 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	movddup	  4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsd    32 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movsd    20 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm0, %xmm3
	movddup	  6 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movsd    24 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movsd    28 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm0, %xmm3
	movddup	 16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movsd    48 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movsd    36 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm1, %xmm2
	movddup	 10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movsd    40 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movsd    44 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm1, %xmm2
	movddup	 12 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movsd    64 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movsd    52 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm1, %xmm3
	movddup	 14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsd    56 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movsd    60 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm1, %xmm3
	movddup	 24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsd    80 * SIZE(BB), %xmm3

	addl	$16 * SIZE, AA
	addl	$64 * SIZE, BB
	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L28
	ALIGN_4

.L26:
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movsd     4 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
 	movddup	  2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsd     8 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	leal	(LDC, LDC, 2), %eax

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0
	movsd	 0 * SIZE(%esi, LDC), %xmm1
	movhps	 2 * SIZE(%esi, LDC), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)
	movlps	%xmm1,   0 * SIZE(%esi, LDC)
	movhps	%xmm1,   2 * SIZE(%esi, LDC)

	movsd	 0 * SIZE(%esi, LDC, 2), %xmm0
	movhps	 2 * SIZE(%esi, LDC, 2), %xmm0
	movsd	 0 * SIZE(%esi, %eax), %xmm1
	movhps	 2 * SIZE(%esi, %eax), %xmm1

	pshufd	$0x50, %xmm5,  %xmm2
	pshufd	$0xfa, %xmm5,  %xmm5

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm5

	addps	%xmm2, %xmm0
	addps	%xmm5, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi, LDC, 2)
	movhps	%xmm0,   2 * SIZE(%esi, LDC, 2)
	movlps	%xmm1,   0 * SIZE(%esi, %eax)
	movhps	%xmm1,   2 * SIZE(%esi, %eax)

	addl	$4 * SIZE, %esi		# coffset += 2
	ALIGN_4

.L30:
	testl	$1, M
	je	.L39

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

	movss	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movss	 4 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movsd	 0 * SIZE(BB), %xmm2
	movsd	16 * SIZE(BB), %xmm3

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
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L35
	ALIGN_4

.L32:
	shufps	$0, %xmm0, %xmm0
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AA)
	movhps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movss	 1 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movsd	 8 * SIZE(BB), %xmm2
	shufps	$0, %xmm0, %xmm0
	movhps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movss	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movhps	20 * SIZE(BB), %xmm3
	shufps	$0, %xmm0, %xmm0
	movsd	32 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	movss	 3 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm4
	movsd	24 * SIZE(BB), %xmm3
	shufps	$0, %xmm0, %xmm0
	movhps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movss	 8 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movsd	48 * SIZE(BB), %xmm3
	shufps	$0, %xmm1, %xmm1
	movhps	36 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movss	 5 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm4
	movsd	40 * SIZE(BB), %xmm2
	shufps	$0, %xmm1, %xmm1
	movhps	44 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movss	 6 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movsd	64 * SIZE(BB), %xmm2
	shufps	$0, %xmm1, %xmm1
	movhps	52 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movss	 7 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movsd	56 * SIZE(BB), %xmm3
	shufps	$0, %xmm1, %xmm1
	movhps	60 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movss	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsd	80 * SIZE(BB), %xmm3

	addl	$ 8 * SIZE, AA
	addl	$64 * SIZE, BB
	decl   %eax
	jne    .L32
	ALIGN_4

.L35:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	shufps	$0, %xmm0, %xmm0
	movhps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movss	 1 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movsd	 8 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
	leal	(LDC, LDC, 2), %eax

	addps	%xmm5, %xmm4

	movsd	 (%esi), %xmm0
	movhps	 (%esi, LDC), %xmm0
	movsd	 (%esi, LDC, 2), %xmm1
	movhps	 (%esi, %eax), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   (%esi)
	movhps	%xmm0,   (%esi, LDC)
	movlps	%xmm1,   (%esi, LDC, 2)
	movhps	%xmm1,   (%esi, %eax)
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	leal	(, LDC, 4), %eax
	addl	%eax, C			# c += 4 * ldc
	decl	J			# j --
	jg	.L01
	ALIGN_4

.L40:
	testl	$2, N
	je	.L80

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$3, %eax
	jle	.L45
	ALIGN_4

.L42:
	movddup	 0 * SIZE(%edi), %xmm0
	movddup	 2 * SIZE(%edi), %xmm1
	movddup	 4 * SIZE(%edi), %xmm2
	movddup	 6 * SIZE(%edi), %xmm3
	movddup	 8 * SIZE(%edi), %xmm4
	movddup	10 * SIZE(%edi), %xmm5
	movddup	12 * SIZE(%edi), %xmm6
	movddup	14 * SIZE(%edi), %xmm7

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)
	movaps	%xmm4, 16 * SIZE(%ecx)
	movaps	%xmm5, 20 * SIZE(%ecx)
	movaps	%xmm6, 24 * SIZE(%ecx)
	movaps	%xmm7, 28 * SIZE(%ecx)

#	prefetcht1	128 * SIZE(%ecx)
	prefetcht0	112 * SIZE(%edi)

	addl	$16 * SIZE, %edi
	addl	$32 * SIZE, %ecx
	decl	%eax
	jne	.L42
	ALIGN_4

.L45:
	movl	K, %eax
	andl	$7, %eax
	BRANCH
	jle	.L50
	ALIGN_4

.L46:
	movddup	 0 * SIZE(%edi), %xmm0
	movaps	%xmm0,  0 * SIZE(%ecx)

	addl	$2 * SIZE, %edi
	addl	$4 * SIZE, %ecx
	decl	%eax
	jne	.L46
	ALIGN_4

.L50:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L51:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

	movaps	  0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	 16 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movsldup  0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movsldup 16 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

	prefetcht2	4 * SIZE(%esi)
	prefetcht2	4 * SIZE(%esi, LDC)

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
	je	.L55
	ALIGN_4

.L52:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AA)
	movshdup  0 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	  4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsldup  4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movshdup  4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	  8 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsldup  8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movshdup  8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	 12 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsldup 12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movshdup 12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	 32 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsldup 32 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movshdup 16 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	 20 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsldup 20 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movshdup 20 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	 24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsldup 24 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movshdup 24 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	 28 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsldup 28 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movshdup 28 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	 48 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsldup 48 * SIZE(BB), %xmm3

	addl	$32 * SIZE, AA
	addl	$32 * SIZE, BB
	decl   %eax
	jne    .L52
	ALIGN_4

.L55:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L58
	ALIGN_4

.L56:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movshdup  0 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	  4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsldup  4 * SIZE(BB), %xmm2

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0
	movsd	 4 * SIZE(%esi), %xmm1
	movhps	 6 * SIZE(%esi), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)
	movlps	%xmm1,   4 * SIZE(%esi)
	movhps	%xmm1,   6 * SIZE(%esi)

	movsd	 0 * SIZE(%esi, LDC), %xmm0
	movhps	 2 * SIZE(%esi, LDC), %xmm0
	movsd	 4 * SIZE(%esi, LDC), %xmm1
	movhps	 6 * SIZE(%esi, LDC), %xmm1

	pshufd	$0x50, %xmm5,  %xmm2
	pshufd	$0xfa, %xmm5,  %xmm5

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm5

	addps	%xmm2, %xmm0
	addps	%xmm5, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi, LDC)
	movhps	%xmm0,   2 * SIZE(%esi, LDC)
	movlps	%xmm1,   4 * SIZE(%esi, LDC)
	movhps	%xmm1,   6 * SIZE(%esi, LDC)

	addl	$8 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L51
	ALIGN_4

.L60:
	testl	$2, M
	je	.L70

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

	movddup	  0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movddup	  8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movsd     0 * SIZE(BB), %xmm2
	movsd    16 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

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
	je	.L65
	ALIGN_4

.L62:
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AA)
	movddup	  2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movsd     4 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	movddup	  4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsd     8 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	movddup	  6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movsd    12 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	movddup	 16 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsd    32 * SIZE(BB), %xmm2
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm1, %xmm3
	movddup	 10 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movsd    20 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm1, %xmm3
	movddup	 12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsd    24 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm1, %xmm3
	movddup	 14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movsd    28 * SIZE(BB), %xmm3
	shufps	$0x50, %xmm3, %xmm3
	mulps	%xmm1, %xmm3
	movddup	 24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsd    48 * SIZE(BB), %xmm3

	addl	$16 * SIZE, AA
	addl	$32 * SIZE, BB
	decl   %eax
	jne    .L62
	ALIGN_4

.L65:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L68
	ALIGN_4

.L66:
	shufps	$0x50, %xmm2, %xmm2
	mulps	%xmm0, %xmm2
	movddup	  2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movsd     4 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L66
	ALIGN_4

.L68:
	addps	%xmm5, %xmm4

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0
	movsd	 0 * SIZE(%esi, LDC), %xmm1
	movhps	 2 * SIZE(%esi, LDC), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)
	movlps	%xmm1,   0 * SIZE(%esi, LDC)
	movhps	%xmm1,   2 * SIZE(%esi, LDC)

	addl	$4 * SIZE, %esi
	ALIGN_4

.L70:
	testl	$1, M
	je	.L79

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

	movss	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movss	 4 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movsd	 0 * SIZE(BB), %xmm2
	movsd	16 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

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
	je	.L75
	ALIGN_4

.L72:
	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm2
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AA)
	movss	 1 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	shufps	$0, %xmm0, %xmm0
	movsd	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movss	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	shufps	$0, %xmm0, %xmm0
	movsd	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movss	 3 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	shufps	$0, %xmm0, %xmm0
	movsd	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movss	 8 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsd	32 * SIZE(BB), %xmm2
	shufps	$0, %xmm1, %xmm1
	mulps	%xmm1, %xmm3
	movss	 5 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	shufps	$0, %xmm1, %xmm1
	movsd	20 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movss	 6 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	shufps	$0, %xmm1, %xmm1
	movsd	24 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movss	 7 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	shufps	$0, %xmm1, %xmm1
	movsd	28 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movss	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsd	48 * SIZE(BB), %xmm3

	addl	$ 8 * SIZE, AA
	addl	$32 * SIZE, BB
	decl   %eax
	jne    .L72
	ALIGN_4

.L75:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_4

.L76:
	shufps	$0, %xmm0, %xmm0
	mulps	%xmm0, %xmm2
	movss	 1 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movsd	 4 * SIZE(BB), %xmm2

	addl	$ 1 * SIZE, AA
	addl	$ 4 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
	addps	%xmm5, %xmm4

	movsd	 (%esi), %xmm0
	movhps	 (%esi, LDC), %xmm0

	pshufd	$0x50, %xmm4,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   (%esi)
	movhps	%xmm0,   (%esi, LDC)
	ALIGN_4

.L79:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif
	leal	(, LDC, 2), %eax
	addl	%eax, C
	ALIGN_4

.L80:
	testl	$1, N
	je	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$3, %eax
	jle	.L85
	ALIGN_4

.L82:
	movss	 0 * SIZE(%edi), %xmm0
	movss	 1 * SIZE(%edi), %xmm1
	movss	 2 * SIZE(%edi), %xmm2
	movss	 3 * SIZE(%edi), %xmm3
	movss	 4 * SIZE(%edi), %xmm4
	movss	 5 * SIZE(%edi), %xmm5
	movss	 6 * SIZE(%edi), %xmm6
	movss	 7 * SIZE(%edi), %xmm7

	movss	%xmm0,  0 * SIZE(%ecx)
	movss	%xmm0,  1 * SIZE(%ecx)
	movss	%xmm1,  2 * SIZE(%ecx)
	movss	%xmm1,  3 * SIZE(%ecx)
	movss	%xmm2,  4 * SIZE(%ecx)
	movss	%xmm2,  5 * SIZE(%ecx)
	movss	%xmm3,  6 * SIZE(%ecx)
	movss	%xmm3,  7 * SIZE(%ecx)
	movss	%xmm4,  8 * SIZE(%ecx)
	movss	%xmm4,  9 * SIZE(%ecx)
	movss	%xmm5, 10 * SIZE(%ecx)
	movss	%xmm5, 11 * SIZE(%ecx)
	movss	%xmm6, 12 * SIZE(%ecx)
	movss	%xmm6, 13 * SIZE(%ecx)
	movss	%xmm7, 14 * SIZE(%ecx)
	movss	%xmm7, 15 * SIZE(%ecx)

#	prefetcht1	128 * SIZE(%ecx)
	prefetcht0	112 * SIZE(%edi)

	addl	$ 8 * SIZE, %edi
	addl	$16 * SIZE, %ecx
	decl	%eax
	jne	.L82
	ALIGN_4

.L85:
	movl	K, %eax
	andl	$7, %eax
	BRANCH
	jle	.L90
	ALIGN_4

.L86:
	movss	 0 * SIZE(%edi), %xmm0
	movss	%xmm0,  0 * SIZE(%ecx)
	movss	%xmm0,  1 * SIZE(%ecx)

	addl	$1 * SIZE, %edi
	addl	$2 * SIZE, %ecx
	decl	%eax
	jne	.L86
	ALIGN_4

.L90:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L100
	ALIGN_4

.L91:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 1), BB
#endif

	movaps	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movddup  0 * SIZE(BB), %xmm2
	pxor	%xmm5, %xmm5
	movaps	16 * SIZE(AA), %xmm1
	movddup  8 * SIZE(BB), %xmm3

#ifdef HAVE_3DNOW
	prefetchw	4 * SIZE(%esi)
#elif defined(HAVE_SSE) || defined(HAVE_SSE2)
	prefetcht2	4 * SIZE(%esi)
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
	je	.L95
	ALIGN_4

.L92:
	mulps	%xmm0, %xmm2
	movaps	 4 * SIZE(AA), %xmm0
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AA)
	addps	%xmm2, %xmm4
	movddup  2 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	 8 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movddup  4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	12 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movddup  6 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	32 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movddup 16 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	movaps	20 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movddup 10 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movddup 12 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	28 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movddup 14 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	48 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movddup 24 * SIZE(BB), %xmm3

	addl	$32 * SIZE, AA
	addl	$16 * SIZE, BB
	decl   %eax
	jne    .L92
	ALIGN_4

.L95:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L98
	ALIGN_4

.L96:
	mulps	%xmm0, %xmm2
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movddup  2 * SIZE(BB), %xmm2

	addl	$4 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L96
	ALIGN_4

.L98:
	addps	%xmm5, %xmm4

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0
	movsd	 4 * SIZE(%esi), %xmm1
	movhps	 6 * SIZE(%esi), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)
	movlps	%xmm1,   4 * SIZE(%esi)
	movhps	%xmm1,   6 * SIZE(%esi)

	addl	$8 * SIZE, %esi
	decl	%ebx			# i --
	jg	.L91
	ALIGN_4

.L100:
	testl	$2, M
	je	.L110

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 1), BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

 	movsd	  0 * SIZE(AA), %xmm0
	movsd     0 * SIZE(BB), %xmm2
 	movsd	  8 * SIZE(AA), %xmm1
	movsd     8 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

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
	je	.L105
	ALIGN_4

.L102:
	mulps	%xmm0, %xmm2
 	movsd	  2 * SIZE(AA), %xmm0
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AA)
	addps	%xmm2, %xmm4
	movsd     2 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
 	movsd	  4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsd     4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
 	movsd	  6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movsd     6 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
 	movsd	 16 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movsd    16 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
 	movsd	 10 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movsd    10 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
 	movsd	 12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsd    12 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
 	movsd	 14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movsd    14 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
 	movsd	 24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movsd    24 * SIZE(BB), %xmm3

	addl	$16 * SIZE, AA
	addl	$16 * SIZE, BB
	decl   %eax
	jne    .L102
	ALIGN_4

.L105:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L108
	ALIGN_4

.L106:
	mulps	%xmm0, %xmm2
 	movsd	  2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movsd     2 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L106
	ALIGN_4

.L108:
	addps	%xmm5, %xmm4
	movhlps	%xmm4, %xmm5
	addps	%xmm5, %xmm4

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0

	pshufd	$0x50, %xmm4,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)

	addl	$4 * SIZE, %esi		# coffset += 2
	ALIGN_4

.L110:
	testl	$1, M
	je	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

 	movss	  0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movss     0 * SIZE(BB), %xmm2
	pxor	%xmm5, %xmm5
 	movss	  4 * SIZE(AA), %xmm1
	movss     8 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

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
	je	.L115
	ALIGN_4

.L112:
	mulss	%xmm0, %xmm2
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AA)
 	movss	  1 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	movss     2 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
 	movss	  2 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm5
	movss     4 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
 	movss	  3 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	movss     6 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
 	movss	  8 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm5
	movss    16 * SIZE(BB), %xmm2
	mulss	%xmm1, %xmm3
 	movss	  5 * SIZE(AA), %xmm1
	addss	%xmm3, %xmm4
	movss    10 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
 	movss	  6 * SIZE(AA), %xmm1
	addss	%xmm3, %xmm5
	movss    12 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
 	movss	  7 * SIZE(AA), %xmm1
	addss	%xmm3, %xmm4
	movss    14 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
 	movss	 12 * SIZE(AA), %xmm1
	addss	%xmm3, %xmm5
	movss    24 * SIZE(BB), %xmm3

	addl	$ 8 * SIZE, AA
	addl	$16 * SIZE, BB
	decl   %eax
	jne    .L112
	ALIGN_4

.L115:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L118
	ALIGN_4

.L116:
	mulss	%xmm0, %xmm2
 	movss	  1 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	movss     2 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L116
	ALIGN_4

.L118:
 	addss	%xmm5, %xmm4

	movsd	 (%esi), %xmm0

	pshufd	$0x50, %xmm4,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   (%esi)
	ALIGN_4

.L999:
	movl	OLD_STACK, %esp
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
