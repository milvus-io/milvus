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

#define PREFETCHSIZE (8 * 4)

#if !defined(HAVE_SSE2) || !defined(HAVE_MMX)
#error  You have to check your configuration.
#endif

#define STACK	16
#define ARGS	 0

#define STACK_M	 4 + STACK + ARGS(%esi)
#define STACK_N	 8 + STACK + ARGS(%esi)
#define STACK_K	12 + STACK + ARGS(%esi)
#define STACK_ALPHA	16 + STACK + ARGS(%esi)
#define STACK_A	24 + STACK + ARGS(%esi)
#define STACK_B	28 + STACK + ARGS(%esi)
#define STACK_C	32 + STACK + ARGS(%esi)
#define STACK_LDC	36 + STACK + ARGS(%esi)
#define STACK_OFFT	40 + STACK + ARGS(%esi)

#define ALPHA	 0(%esp)
#define K	16(%esp)
#define N	20(%esp)
#define M	24(%esp)
#define A	28(%esp)
#define C	32(%esp)
#define J	36(%esp)
#define BX	40(%esp)
#define OLD_STACK 44(%esp)
#define OFFSET  48(%esp)
#define KK	52(%esp)
#define KKK	56(%esp)
#define BUFFER 128(%esp)

#define B	%edi
#define LDC	%ebp

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#define AA	%edx
#define BB	%ecx

#define KERNEL1(address) \
	mulpd	%xmm0, %xmm2; \
	mulpd	 2 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	 0 * SIZE + (address) * SIZE(BB), %xmm2; \
	movq	 (PREFETCHSIZE + 0) * SIZE + (address) * SIZE(AA), %mm2; \
	addpd	%xmm0, %xmm5; \
	movapd	 2 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 2 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	 4 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	 4 * SIZE + (address) * SIZE(AA), %xmm0

#define KERNEL2(address) \
	mulpd	%xmm0, %xmm2; \
	mulpd	 6 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	 4 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm5; \
	movapd	 6 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 6 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	16 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	16 * SIZE + (address) * SIZE(AA), %xmm0

#define KERNEL3(address) \
	movq	 (PREFETCHSIZE + 8) * SIZE + (address) * SIZE(AA), %mm2; \
	mulpd	%xmm1, %xmm3; \
	mulpd	10 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm4; \
	movapd	 8 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm5; \
	movapd	10 * SIZE + (address) * SIZE(AA), %xmm1; \
	mulpd	%xmm1, %xmm3; \
	mulpd	10 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	12 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	12 * SIZE + (address) * SIZE(AA), %xmm1

#define KERNEL4(address) \
	mulpd	%xmm1, %xmm3; \
	mulpd	14 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm4; \
	movapd	12 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm5; \
	movapd	14 * SIZE + (address) * SIZE(AA), %xmm1; \
	mulpd	%xmm1, %xmm3; \
	mulpd	14 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	24 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	24 * SIZE + (address) * SIZE(AA), %xmm1

#define KERNEL5(address) \
	mulpd	%xmm0, %xmm2; \
	mulpd	18 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	16 * SIZE + (address) * SIZE(BB), %xmm2; \
	movq	 (PREFETCHSIZE + 16) * SIZE + (address) * SIZE(AA), %mm2; \
	addpd	%xmm0, %xmm5; \
	movapd	18 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	18 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	20 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	20 * SIZE + (address) * SIZE(AA), %xmm0

#define KERNEL6(address) \
	mulpd	%xmm0, %xmm2; \
	mulpd	22 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	20 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm5; \
	movapd	22 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	22 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	32 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	32 * SIZE + (address) * SIZE(AA), %xmm0

#define KERNEL7(address) \
	movq	 (PREFETCHSIZE + 24) * SIZE + (address) * SIZE(AA), %mm2; \
	mulpd	%xmm1, %xmm3; \
	mulpd	26 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm4; \
	movapd	24 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm5; \
	movapd	26 * SIZE + (address) * SIZE(AA), %xmm1; \
	mulpd	%xmm1, %xmm3; \
	mulpd	26 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	28 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	28 * SIZE + (address) * SIZE(AA), %xmm1

#define KERNEL8(address) \
	mulpd	%xmm1, %xmm3; \
	mulpd	30 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm4; \
	movapd	28 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm5; \
	movapd	30 * SIZE + (address) * SIZE(AA), %xmm1; \
	mulpd	%xmm1, %xmm3; \
	mulpd	30 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	40 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	40 * SIZE + (address) * SIZE(AA), %xmm1

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
	movq	STACK_ALPHA,  %mm7
	movl	STACK_B, B
	movd	STACK_C, %mm3
	movl	STACK_LDC, LDC
#ifdef TRMMKERNEL
	movd	STACK_OFFT, %mm4
#endif

	movq	%mm7, 0 * SIZE + ALPHA
	movq	%mm7, 1 * SIZE + ALPHA

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

	sall	$BASE_SHIFT, LDC
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
	leal	BUFFER, %ecx
	movl	K, %eax
	sarl	$2, %eax
	jle	.L03
	ALIGN_2

.L02:
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1
	movsd	 2 * SIZE(B), %xmm2
	movsd	 3 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm4
	movsd	 5 * SIZE(B), %xmm5
	movsd	 6 * SIZE(B), %xmm6
	movsd	 7 * SIZE(B), %xmm7

	unpcklpd  %xmm0, %xmm0
	unpcklpd  %xmm1, %xmm1
	unpcklpd  %xmm2, %xmm2
	unpcklpd  %xmm3, %xmm3
	unpcklpd  %xmm4, %xmm4
	unpcklpd  %xmm5, %xmm5
	unpcklpd  %xmm6, %xmm6
	unpcklpd  %xmm7, %xmm7

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
	movapd	%xmm2,  4 * SIZE(%ecx)
	movapd	%xmm3,  6 * SIZE(%ecx)
	movapd	%xmm4,  8 * SIZE(%ecx)
	movapd	%xmm5, 10 * SIZE(%ecx)
	movapd	%xmm6, 12 * SIZE(%ecx)
	movapd	%xmm7, 14 * SIZE(%ecx)

	prefetcht0	104 * SIZE(B)

	addl	$  8 * SIZE, B
	subl	$-16 * SIZE, %ecx
	decl	%eax
	BRANCH
	jne	.L02
	ALIGN_2

.L03:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L05
	ALIGN_4

.L04:
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1

	unpcklpd  %xmm0, %xmm0
	unpcklpd  %xmm1, %xmm1

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)

	addl	$2 * SIZE, B
	addl	$4 * SIZE, %ecx
	decl	%eax
	BRANCH
	jne	.L04
	ALIGN_4

.L05:
	movl	B, BX

	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	NOBRANCH
	jle	.L30
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movapd	 0 * SIZE + BUFFER, %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE + BUFFER, %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#endif

	prefetchnta	3 * SIZE(%esi)
	prefetchnta	3 * SIZE(%esi, LDC)

	movl	BX, %eax
	prefetcht2   0 * SIZE(%eax)
	subl	$-8 * SIZE, %eax
	movl	%eax, BX

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


#ifdef PENTIUM4
	andl	$-8, %eax
	NOBRANCH
	je	.L12
	sall	$3, %eax
	.align 8

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

	addl	$64 * 4  * SIZE, AA
	addl	$64 * 4  * SIZE, BB
	subl	$64 * 8, %eax
	BRANCH
	jg	.L1X

.L11:
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB

#else
	sarl	$3, %eax
	je	.L12

.L11:
	KERNEL1(32  *  0)
	KERNEL2(32  *  0)
	KERNEL3(32  *  0)
	KERNEL4(32  *  0)
	KERNEL5(32  *  0)
	KERNEL6(32  *  0)
	KERNEL7(32  *  0)
	KERNEL8(32  *  0)

	addl   $32 * SIZE, %ecx
	addl   $32 * SIZE, %edx
	decl   %eax
	jne    .L11
#endif

.L12:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movapd	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je	.L14

.L13:
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 0 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm6
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm7
	movapd	 4 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA		# aoffset  += 8
	addl	$4 * SIZE, BB		# boffset1 += 8
	subl	$1, %eax
	jg	.L13
	ALIGN_4

.L14:
	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm5
	mulpd	%xmm3, %xmm6
	mulpd	%xmm3, %xmm7

	movl	%esi, %eax
	orl	LDC,  %eax
	testl	$15,  %eax
	NOBRANCH
	jne	.L18x

#ifndef TRMMKERNEL
	movapd	0 * SIZE(%esi), %xmm0
	movapd	2 * SIZE(%esi), %xmm1
	movapd	0 * SIZE(%esi, LDC), %xmm2
	movapd	2 * SIZE(%esi, LDC), %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm6
	addpd	%xmm2, %xmm5
	addpd	%xmm3, %xmm7
#endif

	movapd	%xmm4, 0 * SIZE(%esi)
	movapd	%xmm6, 2 * SIZE(%esi)
	movapd	%xmm5, 0 * SIZE(%esi, LDC)
	movapd	%xmm7, 2 * SIZE(%esi, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif

	addl	$4 * SIZE, %esi		# coffset += 4
	decl	%ebx			# i --
	BRANCH
	jg	.L10
	jmp	.L30
	ALIGN_2

.L18x:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhpd	1 * SIZE(%esi), %xmm0
	movsd	2 * SIZE(%esi), %xmm1
	movhpd	3 * SIZE(%esi), %xmm1

	movsd	0 * SIZE(%esi, LDC), %xmm2
	movhpd	1 * SIZE(%esi, LDC), %xmm2
	movsd	2 * SIZE(%esi, LDC), %xmm3
	movhpd	3 * SIZE(%esi, LDC), %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm6
	addpd	%xmm2, %xmm5
	addpd	%xmm3, %xmm7
#endif

	movsd	%xmm4, 0 * SIZE(%esi)
	movhpd	%xmm4, 1 * SIZE(%esi)
	movsd	%xmm6, 2 * SIZE(%esi)
	movhpd	%xmm6, 3 * SIZE(%esi)

	movsd	%xmm5, 0 * SIZE(%esi, LDC)
	movhpd	%xmm5, 1 * SIZE(%esi, LDC)
	movsd	%xmm7, 2 * SIZE(%esi, LDC)
	movhpd	%xmm7, 3 * SIZE(%esi, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif

	addl	$4 * SIZE, %esi		# coffset += 4
	decl	%ebx			# i --
	BRANCH
	jg	.L10
	ALIGN_2

.L30:
	movl	M,  %ebx
	testl	$2, %ebx
	jle	.L50


#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, %ecx

	movapd	 0 * SIZE + BUFFER, %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE + BUFFER, %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1

	pxor	%xmm7, %xmm7
#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7
#endif

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
	addl	$2, %eax
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L32

.L31:
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm6
	movapd	16 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm7
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	10 * SIZE(BB), %xmm0
	addpd	%xmm3, %xmm4
	movapd	12 * SIZE(BB), %xmm3
	addpd	%xmm0, %xmm5
	movapd	 6 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	14 * SIZE(BB), %xmm0
	addpd	%xmm3, %xmm6
	movapd	24 * SIZE(BB), %xmm3
	addpd	%xmm0, %xmm7
	movapd	16 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	18 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	movapd	20 * SIZE(BB), %xmm2
	addpd	%xmm1, %xmm5
	movapd	10 * SIZE(AA), %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	22 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6
	movapd	32 * SIZE(BB), %xmm2
	addpd	%xmm1, %xmm7
	movapd	12 * SIZE(AA), %xmm1
	mulpd	%xmm1, %xmm3
	mulpd	26 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm4
	movapd	28 * SIZE(BB), %xmm3
	addpd	%xmm1, %xmm5
	movapd	14 * SIZE(AA), %xmm1
	mulpd	%xmm1, %xmm3
	mulpd	30 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm6
	movapd	40 * SIZE(BB), %xmm3
	addpd	%xmm1, %xmm7
	movapd	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $32 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L31

.L32:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movapd	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L34

.L33:
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA		# aoffset  += 8
	addl	$4 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L33
	ALIGN_4

.L34:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm5

#ifndef TRMMKERNEL
	SHUFPD_1 %xmm0, %xmm0
	movsd	0 * SIZE(%esi), %xmm0
	movhpd	1 * SIZE(%esi), %xmm0

	SHUFPD_1 %xmm2, %xmm2
	movsd	0 * SIZE(%esi, LDC), %xmm2
	movhpd	1 * SIZE(%esi, LDC), %xmm2

	addpd	%xmm0, %xmm4
	addpd	%xmm2, %xmm5
#endif

	movsd	%xmm4, 0 * SIZE(%esi)
	movhpd	%xmm4, 1 * SIZE(%esi)

	movsd	%xmm5, 0 * SIZE(%esi, LDC)
	movhpd	%xmm5, 1 * SIZE(%esi, LDC)

	addl	$2 * SIZE, %esi		# coffset += 4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	ALIGN_2

.L50:
	movl	M,  %ebx
	testl	$1, %ebx
	jle	.L99


#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, %ecx

	movapd	 0 * SIZE + BUFFER, %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE + BUFFER, %xmm3
	pxor	%xmm6, %xmm6
	movsd	 4 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movsd	 4 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7
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
	je	.L52

.L51:
	mulsd	%xmm0, %xmm2
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movsd	 1 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm2
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movsd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movsd	 2 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm3
	mulsd	10 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm4
	movsd	12 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm5
	movsd	 3 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm3
	mulsd	14 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm4
	movsd	24 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm5
	movsd	 8 * SIZE(AA), %xmm0
	mulsd	%xmm1, %xmm2
	mulsd	18 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm4
	movsd	20 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	movsd	 5 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm2
	mulsd	22 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm4
	movsd	32 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	movsd	 6 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm3
	mulsd	26 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm4
	movsd	28 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm5
	movsd	 7 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm3
	mulsd	30 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm4
	movsd	40 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm5
	movsd	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $32 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L51

.L52:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movsd	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L54

.L53:
	mulsd	%xmm0, %xmm2
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movsd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA		# aoffset  += 8
	addl	$4 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L53
	ALIGN_4

.L54:
	addsd	%xmm6, %xmm4
	addsd	%xmm7, %xmm5

	mulsd	%xmm3, %xmm4
	mulsd	%xmm3, %xmm5

#ifndef TRMMKERNEL
	addsd	0 * SIZE(%esi), %xmm4
	addsd	0 * SIZE(%esi, LDC), %xmm5
#endif

	movsd	%xmm4, 0 * SIZE(%esi)
	movsd	%xmm5, 0 * SIZE(%esi, LDC)
	addl	$1 * SIZE, %esi

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
	leal	BUFFER, %ecx

	movl	K, %eax
	sarl	$3, %eax
	jle	.L103
	ALIGN_4

.L102:
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1
	movsd	 2 * SIZE(B), %xmm2
	movsd	 3 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm4
	movsd	 5 * SIZE(B), %xmm5
	movsd	 6 * SIZE(B), %xmm6
	movsd	 7 * SIZE(B), %xmm7

	unpcklpd  %xmm0, %xmm0
	unpcklpd  %xmm1, %xmm1
	unpcklpd  %xmm2, %xmm2
	unpcklpd  %xmm3, %xmm3
	unpcklpd  %xmm4, %xmm4
	unpcklpd  %xmm5, %xmm5
	unpcklpd  %xmm6, %xmm6
	unpcklpd  %xmm7, %xmm7

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
	movapd	%xmm2,  4 * SIZE(%ecx)
	movapd	%xmm3,  6 * SIZE(%ecx)
	movapd	%xmm4,  8 * SIZE(%ecx)
	movapd	%xmm5, 10 * SIZE(%ecx)
	movapd	%xmm6, 12 * SIZE(%ecx)
	movapd	%xmm7, 14 * SIZE(%ecx)

	prefetcht0	104 * SIZE(B)

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, %ecx
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
	movsd	 0 * SIZE(B), %xmm0

	unpcklpd  %xmm0, %xmm0

	movapd	%xmm0,  0 * SIZE(%ecx)

	addl	$1 * SIZE, B
	addl	$2 * SIZE, %ecx
	decl	%eax
	jne	.L104
	ALIGN_4

.L105:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L130
	ALIGN_4

.L110:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movapd	 0 * SIZE + BUFFER, %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE + BUFFER, %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7
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
	je	.L112

.L111:
	mulpd	%xmm2, %xmm0
	mulpd	 2 * SIZE(AA), %xmm2
	addpd	%xmm0, %xmm4
	movapd	 4 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm6
	movapd	 2 * SIZE(BB), %xmm2
	mulpd	%xmm2, %xmm0
	mulpd	 6 * SIZE(AA), %xmm2
	addpd	%xmm0, %xmm5
	movapd	16 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm7
	movapd	 4 * SIZE(BB), %xmm2
	mulpd	%xmm2, %xmm1
	mulpd	10 * SIZE(AA), %xmm2
	addpd	%xmm1, %xmm4
	movapd	12 * SIZE(AA), %xmm1
	addpd	%xmm2, %xmm6
	movapd	 6 * SIZE(BB), %xmm2
	mulpd	%xmm2, %xmm1
	mulpd	14 * SIZE(AA), %xmm2
	addpd	%xmm1, %xmm5
	movapd	24 * SIZE(AA), %xmm1
	addpd	%xmm2, %xmm7
	movapd	16 * SIZE(BB), %xmm2
	mulpd	%xmm3, %xmm0
	mulpd	18 * SIZE(AA), %xmm3
	addpd	%xmm0, %xmm4
	movapd	20 * SIZE(AA), %xmm0
	addpd	%xmm3, %xmm6
	movapd	10 * SIZE(BB), %xmm3
	mulpd	%xmm3, %xmm0
	mulpd	22 * SIZE(AA), %xmm3
	addpd	%xmm0, %xmm5
	movapd	32 * SIZE(AA), %xmm0
	addpd	%xmm3, %xmm7
	movapd	12 * SIZE(BB), %xmm3
	mulpd	%xmm3, %xmm1
	mulpd	26 * SIZE(AA), %xmm3
	addpd	%xmm1, %xmm4
	movapd	28 * SIZE(AA), %xmm1
	addpd	%xmm3, %xmm6
	movapd	14 * SIZE(BB), %xmm3
	mulpd	%xmm3, %xmm1
	mulpd	30 * SIZE(AA), %xmm3
	addpd	%xmm1, %xmm5
	movapd	40 * SIZE(AA), %xmm1
	addpd	%xmm3, %xmm7
	movapd	24 * SIZE(BB), %xmm3

	addl   $32 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L111

.L112:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movapd	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L114

.L113:
	mulpd	%xmm2, %xmm0
	mulpd	 2 * SIZE(AA), %xmm2
	addpd	%xmm0, %xmm4
	movapd	 4 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm6
	movapd	 2 * SIZE(BB), %xmm2

	addl	$4 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	subl	$1, %eax
	jg	.L113
	ALIGN_4

.L114:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6

	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm6

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhpd	1 * SIZE(%esi), %xmm0
	addpd	%xmm0, %xmm4

	movsd	2 * SIZE(%esi), %xmm1
	movhpd	3 * SIZE(%esi), %xmm1
	addpd	%xmm1, %xmm6
#endif

	movsd	%xmm4, 0 * SIZE(%esi)
	unpckhpd %xmm4, %xmm4
	movsd	%xmm4, 1 * SIZE(%esi)

	movsd	%xmm6, 2 * SIZE(%esi)
	unpckhpd %xmm6, %xmm6
	movsd	%xmm6, 3 * SIZE(%esi)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif

	addl	$4 * SIZE, %esi		# coffset += 4
	BRANCH
	decl	%ebx			# i --
	jg	.L110
	ALIGN_2

.L130:
	movl	M,  %ebx
	testl	$2, %ebx
	jle	.L150

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movapd	 0 * SIZE + BUFFER, %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE + BUFFER, %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7
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
	je	.L132

.L131:
	mulpd	%xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4
	mulpd	 2 * SIZE(BB), %xmm0
	movapd	16 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	 4 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm6
	movapd	 6 * SIZE(AA), %xmm0
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm7
	movapd	16 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm3
	movapd	10 * SIZE(AA), %xmm1
	addpd	%xmm3, %xmm4
	mulpd	10 * SIZE(BB), %xmm1
	movapd	24 * SIZE(BB), %xmm3
	addpd	%xmm1, %xmm5
	movapd	12 * SIZE(AA), %xmm1
	mulpd	12 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm6
	movapd	14 * SIZE(AA), %xmm1
	mulpd	14 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm7
	movapd	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L131

.L132:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movapd	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L134

.L133:
	movapd	 0 * SIZE(AA), %xmm0
	mulpd	 0 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm4

	addl	$2 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L133
	ALIGN_4

.L134:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
	addpd	%xmm6, %xmm4

	mulpd	%xmm3, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhpd	1 * SIZE(%esi), %xmm0
	addpd	%xmm0, %xmm4
#endif

	movsd	%xmm4, 0 * SIZE(%esi)
	unpckhpd %xmm4, %xmm4
	movsd	%xmm4, 1 * SIZE(%esi)

	addl	$2 * SIZE, %esi		# coffset += 4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif
	ALIGN_2

.L150:
	movl	M,  %ebx
	testl	$1, %ebx
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movapd	 0 * SIZE + BUFFER, %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE + BUFFER, %xmm3
	pxor	%xmm6, %xmm6
	movapd	 4 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 4 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7
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
	je	.L152

.L151:
	mulsd	%xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	mulsd	 2 * SIZE(BB), %xmm0
	movsd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	mulsd	 4 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4
	movsd	 3 * SIZE(AA), %xmm0
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4
	movsd	 8 * SIZE(AA), %xmm0
	mulsd	%xmm1, %xmm3
	movsd	 5 * SIZE(AA), %xmm1
	addsd	%xmm3, %xmm4
	mulsd	10 * SIZE(BB), %xmm1
	movsd	24 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm4
	movsd	 6 * SIZE(AA), %xmm1
	mulsd	12 * SIZE(BB), %xmm1
	addsd	%xmm1, %xmm4
	movsd	 7 * SIZE(AA), %xmm1
	mulsd	14 * SIZE(BB), %xmm1
	addsd	%xmm1, %xmm4
	movsd	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $16 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L151

.L152:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movsd	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L154

.L153:
	movsd	 0 * SIZE(AA), %xmm0
	mulsd	 0 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	addl	$1 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L153
	ALIGN_4

.L154:
	addsd	%xmm6, %xmm4
	addsd	%xmm7, %xmm5

	mulsd	%xmm3, %xmm4

#ifndef TRMMKERNEL
	addsd	0 * SIZE(%esi), %xmm4
#endif
	movsd	%xmm4, 0 * SIZE(%esi)
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
