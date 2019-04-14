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

#define OLD_M	 4 + STACK + ARGS(%esi)
#define OLD_N	 8 + STACK + ARGS(%esi)
#define OLD_K	12 + STACK + ARGS(%esi)
#define OLD_ALPHA_R	16 + STACK + ARGS(%esi)
#define OLD_ALPHA_I	24 + STACK + ARGS(%esi)
#define OLD_A	32 + STACK + ARGS(%esi)
#define OLD_B	36 + STACK + ARGS(%esi)
#define OLD_C	40 + STACK + ARGS(%esi)
#define OLD_LDC	44 + STACK + ARGS(%esi)

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

#if defined(OPTERON) || defined(BARCELONA)
#define movsd  movlpd
#endif

#if defined(OPTERON) || defined(BARCELONA)
#define PREFETCH     prefetch
#define PREFETCHSIZE (8 * 10 + 4)
#endif

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp

#define KERNEL1(address) \
	mulpd	%xmm0, %xmm2; \
	addpd	%xmm2, %xmm4; \
	PREFETCH (PREFETCHSIZE +  0) * SIZE + (address) * 1 * SIZE(AA); \
	movapd	 2 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm0, %xmm2; \
	addpd	%xmm2, %xmm5; \
	movapd	 4 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 6 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	16 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	 2 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL2(address) \
	mulpd	%xmm0, %xmm3; \
	addpd	%xmm3, %xmm4; \
	movapd	10 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm0, %xmm3; \
	addpd	%xmm3, %xmm5; \
	movapd	12 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm0, %xmm3; \
	mulpd	14 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addpd	%xmm3, %xmm6; \
	movapd	24 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addpd	%xmm0, %xmm7; \
	movapd	 4 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL3(address) \
	mulpd	%xmm0, %xmm2; \
	addpd	%xmm2, %xmm4; \
	movapd	18 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm0, %xmm2; \
	addpd	%xmm2, %xmm5; \
	movapd	20 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm0, %xmm2; \
	mulpd	22 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	32 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	 6 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL4(address) \
	mulpd	%xmm0, %xmm3; \
	addpd	%xmm3, %xmm4; \
	movapd	26 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm0, %xmm3; \
	addpd	%xmm3, %xmm5; \
	movapd	28 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm0, %xmm3; \
	mulpd	30 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addpd	%xmm3, %xmm6; \
	movapd	40 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addpd	%xmm0, %xmm7; \
	movapd	16 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL5(address) \
	PREFETCH (PREFETCHSIZE + 8) * SIZE + (address) * 1 * SIZE(AA); \
	mulpd	%xmm1, %xmm2; \
	addpd	%xmm2, %xmm4; \
	movapd	34 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm1, %xmm2; \
	addpd	%xmm2, %xmm5; \
	movapd	36 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm1, %xmm2; \
	mulpd	38 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addpd	%xmm2, %xmm6; \
	movapd	48 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addpd	%xmm1, %xmm7; \
	movapd	10 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL6(address) \
	mulpd	%xmm1, %xmm3; \
	addpd	%xmm3, %xmm4; \
	movapd	42 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm1, %xmm3; \
	addpd	%xmm3, %xmm5; \
	movapd	44 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm1, %xmm3; \
	mulpd	46 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	56 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	12 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL7(address) \
	mulpd	%xmm1, %xmm2; \
	addpd	%xmm2, %xmm4; \
	movapd	50 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm1, %xmm2; \
	addpd	%xmm2, %xmm5; \
	movapd	52 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm1, %xmm2; \
	mulpd	54 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addpd	%xmm2, %xmm6; \
	movapd	64 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addpd	%xmm1, %xmm7; \
	movapd	14 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL8(address) \
	mulpd	%xmm1, %xmm3; \
	addpd	%xmm3, %xmm4; \
	movapd	58 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm1, %xmm3; \
	addpd	%xmm3, %xmm5; \
	movapd	60 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm1, %xmm3; \
	mulpd	62 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	72 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	24 * SIZE + (address) * 1 * SIZE(AA), %xmm1

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	EMMS

	movl	%esp, %esi	# save old stack

	subl	$128 + LOCAL_BUFFER_SIZE, %esp
	andl	$-1024, %esp	# align stack

	STACK_TOUCHING

	movl	OLD_M, %ebx
	movl	OLD_N, %eax
	movl	OLD_K, %ecx
	movl	OLD_A, %edx
	movsd	OLD_ALPHA_R,  %xmm0
	movhps	OLD_ALPHA_I,  %xmm0
	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	movl	%edx, A
	movl	%esi, OLD_STACK
#ifdef TRMMKERNEL
	movss	OLD_OFFT, %xmm4
#endif

	movl	OLD_B, %edi
	movl	OLD_C, %ebx
	movapd	 %xmm0, ALPHA

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
	jle	.L30
	ALIGN_2

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

/* Copying to Sub Buffer */
	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$1, %eax
	jle	.L05
	ALIGN_4

.L02:
#define COPYPREFETCH 40

	prefetchnta	(COPYPREFETCH) * SIZE(%edi)

	movq	 0 * SIZE(%edi), %mm0
	movq	 1 * SIZE(%edi), %mm1
	movq	 2 * SIZE(%edi), %mm2
	movq	 3 * SIZE(%edi), %mm3
	movq	 4 * SIZE(%edi), %mm4
	movq	 5 * SIZE(%edi), %mm5
	movq	 6 * SIZE(%edi), %mm6
	movq	 7 * SIZE(%edi), %mm7

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm0,  1 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)
	movq	%mm1,  3 * SIZE(%ecx)
	movq	%mm2,  4 * SIZE(%ecx)
	movq	%mm2,  5 * SIZE(%ecx)
	movq	%mm3,  6 * SIZE(%ecx)
	movq	%mm3,  7 * SIZE(%ecx)

	movq	%mm4,  8 * SIZE(%ecx)
	movq	%mm4,  9 * SIZE(%ecx)
	movq	%mm5, 10 * SIZE(%ecx)
	movq	%mm5, 11 * SIZE(%ecx)
	movq	%mm6, 12 * SIZE(%ecx)
	movq	%mm6, 13 * SIZE(%ecx)
	movq	%mm7, 14 * SIZE(%ecx)
	movq	%mm7, 15 * SIZE(%ecx)

	addl	$ 8 * SIZE, %edi
	addl	$16 * SIZE, %ecx
	decl	%eax
	jne	.L02
	ALIGN_2

.L05:
	movl	K, %eax
	andl	$1, %eax
	BRANCH
	jle	.L10

	movq	 0 * SIZE(%edi), %mm0
	movq	 1 * SIZE(%edi), %mm1
	movq	 2 * SIZE(%edi), %mm2
	movq	 3 * SIZE(%edi), %mm3

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm0,  1 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)
	movq	%mm1,  3 * SIZE(%ecx)
	movq	%mm2,  4 * SIZE(%ecx)
	movq	%mm2,  5 * SIZE(%ecx)
	movq	%mm3,  6 * SIZE(%ecx)
	movq	%mm3,  7 * SIZE(%ecx)

	addl	$4 * SIZE, %edi
	ALIGN_4

.L10:
	movl	%edi, BX

	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 8), BB
#endif

	movl	BX, %eax

	prefetchnta  0 * SIZE(%eax)
	prefetchnta  8 * SIZE(%eax)

	subl	$-8 * SIZE, BX

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movapd	 0 * SIZE(AA), %xmm0
	movapd	 8 * SIZE(AA), %xmm1
	movapd	 0 * SIZE(BB), %xmm2
	movapd	 8 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

	prefetchw	1 * SIZE(%esi)
	prefetchw	1 * SIZE(%esi, LDC)
	prefetchw	1 * SIZE(%esi, LDC, 2)
	prefetchw	1 * SIZE(%esi, %eax)

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

#if 1
	andl	$-8, %eax
	sall	$4, %eax
	je	.L15
.L1X:
	KERNEL1(16  *  0)
	KERNEL2(16  *  0)
	KERNEL3(16  *  0)
	KERNEL4(16  *  0)
	KERNEL5(16  *  0)
	KERNEL6(16  *  0)
	KERNEL7(16  *  0)
	KERNEL8(16  *  0)
	cmpl	$128 *  1, %eax
	jle	.L12
	KERNEL1(16  *  1)
	KERNEL2(16  *  1)
	KERNEL3(16  *  1)
	KERNEL4(16  *  1)
	KERNEL5(16  *  1)
	KERNEL6(16  *  1)
	KERNEL7(16  *  1)
	KERNEL8(16  *  1)
	cmpl	$128 *  2, %eax
	jle	.L12
	KERNEL1(16  *  2)
	KERNEL2(16  *  2)
	KERNEL3(16  *  2)
	KERNEL4(16  *  2)
	KERNEL5(16  *  2)
	KERNEL6(16  *  2)
	KERNEL7(16  *  2)
	KERNEL8(16  *  2)
	cmpl	$128 *  3, %eax
	jle	.L12
	KERNEL1(16  *  3)
	KERNEL2(16  *  3)
	KERNEL3(16  *  3)
	KERNEL4(16  *  3)
	KERNEL5(16  *  3)
	KERNEL6(16  *  3)
	KERNEL7(16  *  3)
	KERNEL8(16  *  3)
	cmpl	$128 *  4, %eax
	jle	.L12
	KERNEL1(16  *  4)
	KERNEL2(16  *  4)
	KERNEL3(16  *  4)
	KERNEL4(16  *  4)
	KERNEL5(16  *  4)
	KERNEL6(16  *  4)
	KERNEL7(16  *  4)
	KERNEL8(16  *  4)
	cmpl	$128 *  5, %eax
	jle	.L12
	KERNEL1(16  *  5)
	KERNEL2(16  *  5)
	KERNEL3(16  *  5)
	KERNEL4(16  *  5)
	KERNEL5(16  *  5)
	KERNEL6(16  *  5)
	KERNEL7(16  *  5)
	KERNEL8(16  *  5)
	cmpl	$128 *  6, %eax
	jle	.L12
	KERNEL1(16  *  6)
	KERNEL2(16  *  6)
	KERNEL3(16  *  6)
	KERNEL4(16  *  6)
	KERNEL5(16  *  6)
	KERNEL6(16  *  6)
	KERNEL7(16  *  6)
	KERNEL8(16  *  6)
	cmpl	$128 *  7, %eax
	jle	.L12
	KERNEL1(16  *  7)
	KERNEL2(16  *  7)
	KERNEL3(16  *  7)
	KERNEL4(16  *  7)
	KERNEL5(16  *  7)
	KERNEL6(16  *  7)
	KERNEL7(16  *  7)
	KERNEL8(16  *  7)

	addl	$128 * 4  * SIZE, BB
	addl	$128 * 1  * SIZE, AA
	subl	$128 * 8, %eax
	jg	.L1X
	jmp	.L15

.L12:
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
	ALIGN_4
#else

	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	KERNEL1(16  *  0)
	KERNEL2(16  *  0)
	KERNEL3(16  *  0)
	KERNEL4(16  *  0)
	KERNEL5(16  *  0)
	KERNEL6(16  *  0)
	KERNEL7(16  *  0)
	KERNEL8(16  *  0)

	addl   $64 * SIZE, BB
	addl   $16 * SIZE, AA
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
	ALIGN_3

.L16:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movapd	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm5
	movapd	 4 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm6
	movapd	 8 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm7
	movapd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	leal	(LDC, LDC, 2), %eax

	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0
	movsd	2 * SIZE(%esi), %xmm1
	movhps	3 * SIZE(%esi), %xmm1

	pshufd  $0x44,  %xmm4,  %xmm2
	unpckhpd %xmm4, %xmm4

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm4
	addpd	 %xmm4,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi)
	movhps	%xmm0, 1 * SIZE(%esi)
	movlps	%xmm1, 2 * SIZE(%esi)
	movhps	%xmm1, 3 * SIZE(%esi)

	movsd	0 * SIZE(%esi, LDC), %xmm0
	movhps	1 * SIZE(%esi, LDC), %xmm0
	movsd	2 * SIZE(%esi, LDC), %xmm1
	movhps	3 * SIZE(%esi, LDC), %xmm1

	pshufd  $0x44,  %xmm5,  %xmm2
	unpckhpd %xmm5, %xmm5

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm5
	addpd	 %xmm5,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi, LDC)
	movhps	%xmm0, 1 * SIZE(%esi, LDC)
	movlps	%xmm1, 2 * SIZE(%esi, LDC)
	movhps	%xmm1, 3 * SIZE(%esi, LDC)

	movsd	0 * SIZE(%esi, LDC, 2), %xmm0
	movhps	1 * SIZE(%esi, LDC, 2), %xmm0
	movsd	2 * SIZE(%esi, LDC, 2), %xmm1
	movhps	3 * SIZE(%esi, LDC, 2), %xmm1

	pshufd  $0x44,  %xmm6,  %xmm2
	unpckhpd %xmm6, %xmm6

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm6
	addpd	 %xmm6,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi, LDC, 2)
	movhps	%xmm0, 1 * SIZE(%esi, LDC, 2)
	movlps	%xmm1, 2 * SIZE(%esi, LDC, 2)
	movhps	%xmm1, 3 * SIZE(%esi, LDC, 2)

	movsd	0 * SIZE(%esi, %eax), %xmm0
	movhps	1 * SIZE(%esi, %eax), %xmm0
	movsd	2 * SIZE(%esi, %eax), %xmm1
	movhps	3 * SIZE(%esi, %eax), %xmm1

	pshufd  $0x44,  %xmm7,  %xmm2
	unpckhpd %xmm7, %xmm7

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm7
	addpd	 %xmm7,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi, %eax)
	movhps	%xmm0, 1 * SIZE(%esi, %eax)
	movlps	%xmm1, 2 * SIZE(%esi, %eax)
	movhps	%xmm1, 3 * SIZE(%esi, %eax)

	addl	$4 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L11
	ALIGN_4

.L20:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L29

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7
	leal	(LDC, LDC, 2), %eax

	movsd	 0 * SIZE(AA), %xmm0
	movsd	 4 * SIZE(AA), %xmm1
	movsd	 0 * SIZE(BB), %xmm2
	movsd	 8 * SIZE(BB), %xmm3

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
	je	.L25
	ALIGN_4

.L22:
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm4
#if defined(OPTERON) || defined(BARCELONA)
	PREFETCH (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movsd	 2 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm5
	movsd	 4 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm6
	movsd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm7
	movsd	 1 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm4
	movsd	10 * SIZE(BB), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm5
	movsd	12 * SIZE(BB), %xmm3
	mulsd	%xmm0, %xmm3
	mulsd	14 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm6
	movsd	24 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm7
	movsd	 2 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm4
	movsd	18 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm5
	movsd	20 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	mulsd	22 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm6
	movsd	32 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm7
	movsd	 3 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm4
	movsd	26 * SIZE(BB), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm5
	movsd	28 * SIZE(BB), %xmm3
	mulsd	%xmm0, %xmm3
	mulsd	30 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm6
	movsd	40 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm7
	movsd	 8 * SIZE(AA), %xmm0
#if defined(OPTERON) || defined(BARCELONA)
	PREFETCH (PREFETCHSIZE  + 8) * SIZE(AA)
#endif
	mulsd	%xmm1, %xmm2
	addsd	%xmm2, %xmm4
	movsd	34 * SIZE(BB), %xmm2
	mulsd	%xmm1, %xmm2
	addsd	%xmm2, %xmm5
	movsd	36 * SIZE(BB), %xmm2
	mulsd	%xmm1, %xmm2
	mulsd	38 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm6
	movsd	48 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm7
	movsd	 5 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm3
	addsd	%xmm3, %xmm4
	movsd	42 * SIZE(BB), %xmm3
	mulsd	%xmm1, %xmm3
	addsd	%xmm3, %xmm5
	movsd	44 * SIZE(BB), %xmm3
	mulsd	%xmm1, %xmm3
	mulsd	46 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm6
	movsd	56 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm7
	movsd	 6 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm2
	addsd	%xmm2, %xmm4
	movsd	50 * SIZE(BB), %xmm2
	mulsd	%xmm1, %xmm2
	addsd	%xmm2, %xmm5
	movsd	52 * SIZE(BB), %xmm2
	mulsd	%xmm1, %xmm2
	mulsd	54 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm6
	movsd	64 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm7
	movsd	 7 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm3
	addsd	%xmm3, %xmm4
	movsd	58 * SIZE(BB), %xmm3
	mulsd	%xmm1, %xmm3
	addsd	%xmm3, %xmm5
	movsd	60 * SIZE(BB), %xmm3
	mulsd	%xmm1, %xmm3
	mulsd	62 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm6
	movsd	72 * SIZE(BB), %xmm3
	addl   $64 * SIZE, BB
	addsd	%xmm1, %xmm7
	movsd	12 * SIZE(AA), %xmm1
	addl   $8 * SIZE, AA
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

.L26:
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm4
	movsd	 2 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm5
	movsd	 4 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 8 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm7
	movsd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	leal	(LDC, LDC, 2), %eax

	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0
	movsd	0 * SIZE(%esi, LDC), %xmm1
	movhps	1 * SIZE(%esi, LDC), %xmm1

	unpcklpd %xmm4, %xmm4
	unpcklpd %xmm5, %xmm5

	mulpd	 %xmm3,  %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm3,  %xmm5
	addpd	 %xmm5,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi)
	movhps	%xmm0, 1 * SIZE(%esi)
	movlps	%xmm1, 0 * SIZE(%esi, LDC)
	movhps	%xmm1, 1 * SIZE(%esi, LDC)

	movsd	0 * SIZE(%esi, LDC, 2), %xmm0
	movhps	1 * SIZE(%esi, LDC, 2), %xmm0
	movsd	0 * SIZE(%esi, %eax), %xmm1
	movhps	1 * SIZE(%esi, %eax), %xmm1

	unpcklpd %xmm6, %xmm6
	unpcklpd %xmm7, %xmm7

	mulpd	 %xmm3,  %xmm6
	addpd	 %xmm6,  %xmm0
	mulpd	 %xmm3,  %xmm7
	addpd	 %xmm7,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi, LDC, 2)
	movhps	%xmm0, 1 * SIZE(%esi, LDC, 2)
	movlps	%xmm1, 0 * SIZE(%esi, %eax)
	movhps	%xmm1, 1 * SIZE(%esi, %eax)
	ALIGN_4

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	leal	(, LDC, 4), %eax
	addl	%eax, C			# c += 4 * ldc
	decl	J			# j --
	jg	.L01
	ALIGN_4

.L30:
	testl	$2, N
	je	.L60
	ALIGN_2

.L31:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

/* Copying to Sub Buffer */
	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$2, %eax
	jle	.L35
	ALIGN_4

.L32:
#ifdef PENTIUM4
#ifdef HAVE_SSE3
        movddup	 0 * SIZE(%edi), %xmm0
	movddup	 1 * SIZE(%edi), %xmm1
	movddup	 2 * SIZE(%edi), %xmm2
	movddup	 3 * SIZE(%edi), %xmm3
	movddup	 4 * SIZE(%edi), %xmm4
	movddup	 5 * SIZE(%edi), %xmm5
	movddup	 6 * SIZE(%edi), %xmm6
	movddup	 7 * SIZE(%edi), %xmm7

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
	movapd	%xmm2,  4 * SIZE(%ecx)
	movapd	%xmm3,  6 * SIZE(%ecx)
	movapd	%xmm4,  8 * SIZE(%ecx)
	movapd	%xmm5, 10 * SIZE(%ecx)
	movapd	%xmm6, 12 * SIZE(%ecx)
	movapd	%xmm7, 14 * SIZE(%ecx)
#else
	movsd	 0 * SIZE(%edi), %xmm0
	movsd	 1 * SIZE(%edi), %xmm1
	movsd	 2 * SIZE(%edi), %xmm2
	movsd	 3 * SIZE(%edi), %xmm3
	movsd	 4 * SIZE(%edi), %xmm4
	movsd	 5 * SIZE(%edi), %xmm5
	movsd	 6 * SIZE(%edi), %xmm6
	movsd	 7 * SIZE(%edi), %xmm7

	unpcklpd  %xmm0, %xmm0
	unpckhpd  %xmm1, %xmm1
	unpcklpd  %xmm2, %xmm2
	unpckhpd  %xmm3, %xmm3
	unpcklpd  %xmm4, %xmm4
	unpckhpd  %xmm5, %xmm5
	unpcklpd  %xmm6, %xmm6
	unpckhpd  %xmm7, %xmm7

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
	movapd	%xmm2,  4 * SIZE(%ecx)
	movapd	%xmm3,  6 * SIZE(%ecx)
	movapd	%xmm4,  8 * SIZE(%ecx)
	movapd	%xmm5, 10 * SIZE(%ecx)
	movapd	%xmm6, 12 * SIZE(%ecx)
	movapd	%xmm7, 14 * SIZE(%ecx)
#endif
	prefetcht0	 80 * SIZE(%edi)
     	prefetcht1	112 * SIZE(%ecx)
#endif

#if defined(OPTERON) || defined(BARCELONA)
#define COPYPREFETCH 40

	prefetchnta	(COPYPREFETCH) * SIZE(%edi)

	movq	 0 * SIZE(%edi), %mm0
	movq	 1 * SIZE(%edi), %mm1
	movq	 2 * SIZE(%edi), %mm2
	movq	 3 * SIZE(%edi), %mm3
	movq	 4 * SIZE(%edi), %mm4
	movq	 5 * SIZE(%edi), %mm5
	movq	 6 * SIZE(%edi), %mm6
	movq	 7 * SIZE(%edi), %mm7

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm0,  1 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)
	movq	%mm1,  3 * SIZE(%ecx)
	movq	%mm2,  4 * SIZE(%ecx)
	movq	%mm2,  5 * SIZE(%ecx)
	movq	%mm3,  6 * SIZE(%ecx)
	movq	%mm3,  7 * SIZE(%ecx)

	movq	%mm4,  8 * SIZE(%ecx)
	movq	%mm4,  9 * SIZE(%ecx)
	movq	%mm5, 10 * SIZE(%ecx)
	movq	%mm5, 11 * SIZE(%ecx)
	movq	%mm6, 12 * SIZE(%ecx)
	movq	%mm6, 13 * SIZE(%ecx)
	movq	%mm7, 14 * SIZE(%ecx)
	movq	%mm7, 15 * SIZE(%ecx)
#endif
	addl	$ 8 * SIZE, %edi
	addl	$16 * SIZE, %ecx
	decl	%eax
	jne	.L32
	ALIGN_2

.L35:
	movl	K,  %eax
	andl	$3, %eax
	BRANCH
	jle	.L40
	ALIGN_2

.L36:
#ifdef PENTIUM4
#ifdef HAVE_SSE3
	movddup	 0 * SIZE(%edi), %xmm0
	movddup	 1 * SIZE(%edi), %xmm1

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
#else
	movsd	 0 * SIZE(%edi), %xmm0
	movsd	 1 * SIZE(%edi), %xmm1

	unpcklpd  %xmm0, %xmm0
	unpckhpd  %xmm1, %xmm1

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
#endif
#endif

#if defined(OPTERON) || defined(BARCELONA)
	movq	 0 * SIZE(%edi), %mm0
	movq	 1 * SIZE(%edi), %mm1

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm0,  1 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)
	movq	%mm1,  3 * SIZE(%ecx)
#endif
	addl	$2 * SIZE, %edi
	addl	$4 * SIZE, %ecx
	decl	%eax
	jne	.L36
	ALIGN_4

.L40:
	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L50
	ALIGN_4

.L41:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movapd	 0 * SIZE(AA), %xmm0
	movapd	 8 * SIZE(AA), %xmm1
	movapd	 0 * SIZE(BB), %xmm2
	movapd	 8 * SIZE(BB), %xmm3

#ifdef HAVE_3DNOW
	prefetchw 2 * SIZE(%esi)
	prefetchw 2 * SIZE(%esi, LDC)
#endif

#ifdef PENTIUM4
	prefetchnta	4 * SIZE(%esi)
	prefetchnta	4 * SIZE(%esi, LDC)
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
	je	.L45
	ALIGN_4

.L42:
	mulpd	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
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

#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 8) * SIZE(AA)
#endif
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
	decl   %eax
	jne    .L42
	ALIGN_4

.L45:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L48
	ALIGN_3

.L46:
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0
	movsd	2 * SIZE(%esi), %xmm1
	movhps	3 * SIZE(%esi), %xmm1

	pshufd  $0x44,  %xmm4,  %xmm2
	unpckhpd %xmm4, %xmm4

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm4
	addpd	 %xmm4,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi)
	movhps	%xmm0, 1 * SIZE(%esi)
	movlps	%xmm1, 2 * SIZE(%esi)
	movhps	%xmm1, 3 * SIZE(%esi)

	movsd	0 * SIZE(%esi, LDC), %xmm0
	movhps	1 * SIZE(%esi, LDC), %xmm0
	movsd	2 * SIZE(%esi, LDC), %xmm1
	movhps	3 * SIZE(%esi, LDC), %xmm1

	pshufd  $0x44,  %xmm5,  %xmm2
	unpckhpd %xmm5, %xmm5

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm5
	addpd	 %xmm5,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi, LDC)
	movhps	%xmm0, 1 * SIZE(%esi, LDC)
	movlps	%xmm1, 2 * SIZE(%esi, LDC)
	movhps	%xmm1, 3 * SIZE(%esi, LDC)

	addl	$4 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L41
	ALIGN_4

.L50:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L59

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	leal	(LDC, LDC, 2), %eax

	movsd	 0 * SIZE(AA), %xmm0
	movsd	 4 * SIZE(AA), %xmm1
	movsd	 0 * SIZE(BB), %xmm2
	movsd	 8 * SIZE(BB), %xmm3

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
	je	.L55
	ALIGN_4

.L52:
	mulsd	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA)
	PREFETCH (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movsd	 1 * SIZE(AA), %xmm0

	mulsd	%xmm0, %xmm2
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm6
	movsd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm7
	movsd	 2 * SIZE(AA), %xmm0

	mulsd	%xmm0, %xmm3
	mulsd	10 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm4
	movsd	12 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm5
	movsd	 3 * SIZE(AA), %xmm0

	mulsd	%xmm0, %xmm3
	mulsd	14 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm6
	movsd	24 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm7
	movsd	 8 * SIZE(AA), %xmm0

	mulsd	%xmm1, %xmm2
	mulsd	18 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm4
	movsd	20 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	movsd	 5 * SIZE(AA), %xmm1

	mulsd	%xmm1, %xmm2
	mulsd	22 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm6
	movsd	32 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm7
	movsd	 6 * SIZE(AA), %xmm1

	mulsd	%xmm1, %xmm3
	mulsd	26 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm4
	movsd	28 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm5
	movsd	 7 * SIZE(AA), %xmm1

	mulsd	%xmm1, %xmm3
	mulsd	30 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm6
	movsd	40 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm7
	movsd	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $32 * SIZE, BB
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

.L56:
	mulsd	%xmm0, %xmm2
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movsd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	addsd	%xmm6, %xmm4
	addsd	%xmm7, %xmm5

	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0
	movsd	0 * SIZE(%esi, LDC), %xmm1
	movhps	1 * SIZE(%esi, LDC), %xmm1

	unpcklpd %xmm4, %xmm4
	unpcklpd %xmm5, %xmm5

	mulpd	 %xmm3,  %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm3,  %xmm5
	addpd	 %xmm5,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi)
	movhps	%xmm0, 1 * SIZE(%esi)
	movlps	%xmm1, 0 * SIZE(%esi, LDC)
	movhps	%xmm1, 1 * SIZE(%esi, LDC)

	ALIGN_4

.L59:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leal	(, LDC, 2), %eax
	addl	%eax, C			# c += 4 * ldc
	ALIGN_4

.L60:
	testl	$1, N
	je	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$3, %eax
	jle	.L65
	ALIGN_4

.L62:
#ifdef PENTIUM4
#ifdef HAVE_SSE3
        movddup	 0 * SIZE(%edi), %xmm0
	movddup	 1 * SIZE(%edi), %xmm1
	movddup	 2 * SIZE(%edi), %xmm2
	movddup	 3 * SIZE(%edi), %xmm3
	movddup	 4 * SIZE(%edi), %xmm4
	movddup	 5 * SIZE(%edi), %xmm5
	movddup	 6 * SIZE(%edi), %xmm6
	movddup	 7 * SIZE(%edi), %xmm7

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
	movapd	%xmm2,  4 * SIZE(%ecx)
	movapd	%xmm3,  6 * SIZE(%ecx)
	movapd	%xmm4,  8 * SIZE(%ecx)
	movapd	%xmm5, 10 * SIZE(%ecx)
	movapd	%xmm6, 12 * SIZE(%ecx)
	movapd	%xmm7, 14 * SIZE(%ecx)
#else
	movsd	 0 * SIZE(%edi), %xmm0
	movsd	 1 * SIZE(%edi), %xmm1
	movsd	 2 * SIZE(%edi), %xmm2
	movsd	 3 * SIZE(%edi), %xmm3
	movsd	 4 * SIZE(%edi), %xmm4
	movsd	 5 * SIZE(%edi), %xmm5
	movsd	 6 * SIZE(%edi), %xmm6
	movsd	 7 * SIZE(%edi), %xmm7

	unpcklpd  %xmm0, %xmm0
	unpckhpd  %xmm1, %xmm1
	unpcklpd  %xmm2, %xmm2
	unpckhpd  %xmm3, %xmm3
	unpcklpd  %xmm4, %xmm4
	unpckhpd  %xmm5, %xmm5
	unpcklpd  %xmm6, %xmm6
	unpckhpd  %xmm7, %xmm7

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
	movapd	%xmm2,  4 * SIZE(%ecx)
	movapd	%xmm3,  6 * SIZE(%ecx)
	movapd	%xmm4,  8 * SIZE(%ecx)
	movapd	%xmm5, 10 * SIZE(%ecx)
	movapd	%xmm6, 12 * SIZE(%ecx)
	movapd	%xmm7, 14 * SIZE(%ecx)
#endif
	prefetcht1	 80 * SIZE(%edi)
     	prefetcht0	112 * SIZE(%ecx)
#endif

#if defined(OPTERON) || defined(BARCELONA)
#define COPYPREFETCH 40

	prefetchnta	(COPYPREFETCH) * SIZE(%edi)

	movq	 0 * SIZE(%edi), %mm0
	movq	 1 * SIZE(%edi), %mm1
	movq	 2 * SIZE(%edi), %mm2
	movq	 3 * SIZE(%edi), %mm3
	movq	 4 * SIZE(%edi), %mm4
	movq	 5 * SIZE(%edi), %mm5
	movq	 6 * SIZE(%edi), %mm6
	movq	 7 * SIZE(%edi), %mm7

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm0,  1 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)
	movq	%mm1,  3 * SIZE(%ecx)
	movq	%mm2,  4 * SIZE(%ecx)
	movq	%mm2,  5 * SIZE(%ecx)
	movq	%mm3,  6 * SIZE(%ecx)
	movq	%mm3,  7 * SIZE(%ecx)

	movq	%mm4,  8 * SIZE(%ecx)
	movq	%mm4,  9 * SIZE(%ecx)
	movq	%mm5, 10 * SIZE(%ecx)
	movq	%mm5, 11 * SIZE(%ecx)
	movq	%mm6, 12 * SIZE(%ecx)
	movq	%mm6, 13 * SIZE(%ecx)
	movq	%mm7, 14 * SIZE(%ecx)
	movq	%mm7, 15 * SIZE(%ecx)
#endif
	addl	$ 8 * SIZE, %edi
	addl	$16 * SIZE, %ecx
	decl	%eax
	jne	.L62
	ALIGN_2

.L65:
	movl	K,  %eax
	andl	$7, %eax
	BRANCH
	jle	.L70
	ALIGN_2

.L66:
#ifdef PENTIUM4
#ifdef HAVE_SSE3
	movddup	 0 * SIZE(%edi), %xmm0
	movapd	%xmm0,  0 * SIZE(%ecx)
#else
	movsd	 0 * SIZE(%edi), %xmm0
	unpcklpd  %xmm0, %xmm0
	movapd	%xmm0,  0 * SIZE(%ecx)
#endif
#endif

#if defined(OPTERON) || defined(BARCELONA)
	movq	 0 * SIZE(%edi), %mm0

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm0,  1 * SIZE(%ecx)
#endif
	addl	$1 * SIZE, %edi
	addl	$2 * SIZE, %ecx
	decl	%eax
	jne	.L66
	ALIGN_4

.L70:
	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L80
	ALIGN_4

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movapd	 0 * SIZE(AA), %xmm0
	movapd	 8 * SIZE(AA), %xmm1
	movapd	 0 * SIZE(BB), %xmm2
	movapd	 8 * SIZE(BB), %xmm3

#ifdef HAVE_3DNOW
	prefetchw   2 * SIZE(%esi)
#endif

#ifdef PENTIUM4
	prefetchnta 2 * SIZE(%esi)
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
	je	.L75
	ALIGN_4

.L72:
	mulpd	%xmm0, %xmm2
	addpd	%xmm2, %xmm4
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movapd	16 * SIZE(BB), %xmm2

	movapd	 2 * SIZE(AA), %xmm0
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm4
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	 4 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm4
	movapd	 6 * SIZE(AA), %xmm0
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm4

	movapd	16 * SIZE(AA), %xmm0
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 8) * SIZE(AA)
#endif
	mulpd	%xmm1, %xmm3
	addpd	%xmm3, %xmm4
	movapd	24 * SIZE(BB), %xmm3

	movapd	10 * SIZE(AA), %xmm1
	mulpd	10 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm4
	movapd	12 * SIZE(AA), %xmm1
	mulpd	12 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm4
	movapd	14 * SIZE(AA), %xmm1
	mulpd	14 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm4
	movapd	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
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
	ALIGN_3

.L76:
	mulpd	%xmm0, %xmm2
	addpd	%xmm2, %xmm4
	movapd	 2 * SIZE(AA), %xmm0
	movapd	 2 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0
	movsd	2 * SIZE(%esi), %xmm1
	movhps	3 * SIZE(%esi), %xmm1

	pshufd  $0x44,  %xmm4,  %xmm2
	unpckhpd %xmm4, %xmm4

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm4
	addpd	 %xmm4,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi)
	movhps	%xmm0, 1 * SIZE(%esi)
	movlps	%xmm1, 2 * SIZE(%esi)
	movhps	%xmm1, 3 * SIZE(%esi)

	addl	$4 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L71
	ALIGN_4

.L80:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	leal	(LDC, LDC, 2), %eax

	movsd	 0 * SIZE(AA), %xmm0
	movsd	 4 * SIZE(AA), %xmm1
	movsd	 0 * SIZE(BB), %xmm2
	movsd	 8 * SIZE(BB), %xmm3

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
	je	.L85
	ALIGN_4

.L82:
	mulsd	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movsd	 1 * SIZE(AA), %xmm0
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movsd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movsd	 2 * SIZE(AA), %xmm0
	mulsd	 4 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm6
	movsd	 3 * SIZE(AA), %xmm0
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm7
	movsd	 8 * SIZE(AA), %xmm0
	mulsd	%xmm1, %xmm3
	movsd	 5 * SIZE(AA), %xmm1
	mulsd	10 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm4
	movsd	24 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm5
	movsd	 6 * SIZE(AA), %xmm1
	mulsd	12 * SIZE(BB), %xmm1
	addsd	%xmm1, %xmm6
	movsd	 7 * SIZE(AA), %xmm1
	mulsd	14 * SIZE(BB), %xmm1
	addsd	%xmm1, %xmm7
	movsd	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L82
	ALIGN_4

.L85:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L88

.L86:
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm4
	movsd	 2 * SIZE(BB), %xmm2
	movsd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L86
	ALIGN_4

.L88:
	addsd	%xmm5, %xmm4
	addsd	%xmm7, %xmm6
	addsd	%xmm6, %xmm4

	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0

	pshufd  $0x44,  %xmm4,  %xmm2
	unpckhpd %xmm4, %xmm4

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0

	movlps	%xmm0, 0 * SIZE(%esi)
	movhps	%xmm0, 1 * SIZE(%esi)
	ALIGN_4

.L999:
	movl	OLD_STACK, %esp
	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
