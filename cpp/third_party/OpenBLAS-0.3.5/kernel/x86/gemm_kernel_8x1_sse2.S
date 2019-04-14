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

#define ALPHA	 0(%esp)
#define K	16(%esp)
#define N	20(%esp)
#define M	24(%esp)
#define A	28(%esp)
#define C	36(%esp)
#define J	44(%esp)
#define OLD_STACK 48(%esp)
#define BUFFER 128(%esp)

#define B	%edi
#define LDC	%ebp

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#define AA	%edx
#define BB	%ecx

#define KERNELMACRO(address) \
	mulpd	%xmm0, %xmm2; \
	mulpd	 2 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	 0 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm5; \
	movapd	 2 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 2 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	 4 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	 4 * SIZE + (address) * SIZE(AA), %xmm0; \
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
	movapd	16 * SIZE + (address) * SIZE(AA), %xmm0; \
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
	movapd	12 * SIZE + (address) * SIZE(AA), %xmm1; \
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
	movapd	24 * SIZE + (address) * SIZE(AA), %xmm1; \
	mulpd	%xmm0, %xmm2; \
	mulpd	18 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	16 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm5; \
	movapd	18 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	18 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	20 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	20 * SIZE + (address) * SIZE(AA), %xmm0; \
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
	movapd	32 * SIZE + (address) * SIZE(AA), %xmm0; \
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
	movapd	28 * SIZE + (address) * SIZE(AA), %xmm1; \
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

	movq	%mm7, 0 * SIZE + ALPHA
	movq	%mm7, 1 * SIZE + ALPHA

	movd	%mm1, K
	movl	%eax, N
	movd	%mm0, M
	movd	%mm2, A
	movd	%mm3, C
	movl	%esi, OLD_STACK

	leal	(, LDC, SIZE), LDC

	test	%eax, %eax
	movl	%eax, J
	jle	.L999
	ALIGN_2

.L01:
/* Copying to Sub Buffer */
	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$3, %eax
	jle	.L03
	ALIGN_4

.L02:
	prefetchnta	 96 * SIZE(B)

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

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, %ecx
	decl	%eax
	BRANCH
	jne	.L02
	ALIGN_2

.L03:
	movl	K, %eax
	andl	$7, %eax
	BRANCH
	jle	.L05
	ALIGN_2

.L04:
	movsd	 0 * SIZE(B), %xmm0
	unpcklpd  %xmm0, %xmm0
	movapd	%xmm0,  0 * SIZE(%ecx)

	addl	$1 * SIZE, B
	addl	$2 * SIZE, %ecx
	decl	%eax
	jne	.L04
	ALIGN_4

.L05:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$3, %ebx	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L10:
	leal	BUFFER, %ecx	# boffset1 = boffset // different point
	movl	K, %eax

	movapd	 0 * SIZE + BUFFER, %xmm2
	movapd	 0 * SIZE(%edx), %xmm0
	movapd	 8 * SIZE + BUFFER, %xmm3
	movapd	 8 * SIZE(%edx), %xmm1

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#if 0
	andl	$-8, %eax
	leal	(, %eax, 8), %eax
	je	.L12

	KERNELMACRO(32 *  0)		# 0
	cmpl	$64 *  1, %eax
	jle	.L11
	KERNELMACRO(32 *  1)		# 1
	cmpl	$64 *  2, %eax
	jle	.L11
	KERNELMACRO(32 *  2)		# 2
	cmpl	$64 *  3, %eax
	jle	.L11
	KERNELMACRO(32 *  3)		# 3
	cmpl	$64 *  4, %eax
	jle	.L11
	KERNELMACRO(32 *  4)		# 4
	cmpl	$64 *  5, %eax
	jle	.L11
	KERNELMACRO(32 *  5)		# 5
	cmpl	$64 *  6, %eax
	jle	.L11
	KERNELMACRO(32 *  6)		# 6
	cmpl	$64 *  7, %eax
	jle	.L11
	KERNELMACRO(32 *  7)		# 7
	cmpl	$64 *  8, %eax
	jle	.L11
	KERNELMACRO(32 *  8)		# 8
	cmpl	$64 *  9, %eax
	jle	.L11
	KERNELMACRO(32 *  9)		# 9
	cmpl	$64 * 10, %eax
	jle	.L11
	KERNELMACRO(32 * 10)		# 10
	cmpl	$64 * 11, %eax
	jle	.L11
	KERNELMACRO(32 * 11)		# 11
	cmpl	$64 * 12, %eax
	jle	.L11
	KERNELMACRO(32 * 12)		# 12
	cmpl	$64 * 13, %eax
	jle	.L11
	KERNELMACRO(32 * 13)		# 13
	cmpl	$64 * 14, %eax
	jle	.L11
	KERNELMACRO(32 * 14)		# 14
	cmpl	$64 * 15, %eax
	jle	.L11
	movq	1 * SIZE(%esi), %mm0
	movq	1 * SIZE(%esi, LDC), %mm1
	KERNELMACRO(32 * 15)		# 15
.L11:
	leal	(%edx, %eax, 4), %edx
	leal	(%ecx, %eax, 4), %ecx

#else
	movapd	 0 * SIZE(BB), %xmm0
	movapd	 8 * SIZE(BB), %xmm2
	movapd	 0 * SIZE(AA), %xmm1
	movapd	 8 * SIZE(AA), %xmm3

	prefetchnta 8 * SIZE(%esi)

	sarl	$3, %eax
	je	.L12

#define PRE 40

.L11:
	mulpd	%xmm0, %xmm1
	movd	(PRE + 0) * SIZE(AA), %mm0
	addpd	%xmm1, %xmm4
	movapd	 2 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm5
	movapd	 4 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	 6 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm6
	movapd	16 * SIZE(AA), %xmm1
	movd	(PRE + 8) * SIZE(AA), %mm0
	addpd	%xmm0, %xmm7
	movapd	 2 * SIZE(BB), %xmm0
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm4
	movapd	10 * SIZE(AA), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm5
	movapd	12 * SIZE(AA), %xmm3
	mulpd	%xmm0, %xmm3
	mulpd	14 * SIZE(AA), %xmm0
	addpd	%xmm3, %xmm6
	movapd	24 * SIZE(AA), %xmm3
	movd	(PRE + 16) * SIZE(AA), %mm0
	addpd	%xmm0, %xmm7
	movapd	 4 * SIZE(BB), %xmm0
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movapd	18 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm5
	movapd	20 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	mulpd	22 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm6
	movapd	32 * SIZE(AA), %xmm1
	movd	(PRE + 24) * SIZE(AA), %mm0
	addpd	%xmm0, %xmm7
	movapd	 6 * SIZE(BB), %xmm0
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm4
	movapd	26 * SIZE(AA), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm3, %xmm5
	movapd	28 * SIZE(AA), %xmm3
	mulpd	%xmm0, %xmm3
	mulpd	30 * SIZE(AA), %xmm0
	addpd	%xmm3, %xmm6
	movapd	40 * SIZE(AA), %xmm3
	movd	(PRE + 32) * SIZE(AA), %mm0
	addpd	%xmm0, %xmm7
	movapd	16 * SIZE(BB), %xmm0
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm4
	movapd	34 * SIZE(AA), %xmm1
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm5
	movapd	36 * SIZE(AA), %xmm1
	mulpd	%xmm2, %xmm1
	mulpd	38 * SIZE(AA), %xmm2
	addpd	%xmm1, %xmm6
	movapd	48 * SIZE(AA), %xmm1
	movd	(PRE + 40) * SIZE(AA), %mm0
	addpd	%xmm2, %xmm7
	movapd	10 * SIZE(BB), %xmm2
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm4
	movapd	42 * SIZE(AA), %xmm3
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm5
	movapd	44 * SIZE(AA), %xmm3
	mulpd	%xmm2, %xmm3
	mulpd	46 * SIZE(AA), %xmm2
	addpd	%xmm3, %xmm6
	movapd	56 * SIZE(AA), %xmm3
	movd	(PRE + 48) * SIZE(AA), %mm0
	addpd	%xmm2, %xmm7
	movapd	12 * SIZE(BB), %xmm2
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm4
	movapd	50 * SIZE(AA), %xmm1
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm5
	movapd	52 * SIZE(AA), %xmm1
	mulpd	%xmm2, %xmm1
	mulpd	54 * SIZE(AA), %xmm2
	addpd	%xmm1, %xmm6
	movapd	64 * SIZE(AA), %xmm1
	movd	(PRE + 56) * SIZE(AA), %mm0
	addpd	%xmm2, %xmm7
	movapd	14 * SIZE(BB), %xmm2
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm4
	movapd	58 * SIZE(AA), %xmm3
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm5
	movapd	60 * SIZE(AA), %xmm3
	mulpd	%xmm2, %xmm3
	mulpd	62 * SIZE(AA), %xmm2
	addpd	%xmm3, %xmm6
	movapd	72 * SIZE(AA), %xmm3
	addpd	%xmm2, %xmm7
	movapd	24 * SIZE(BB), %xmm2

	addl   $64 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L11
#endif

.L12:
	movapd	ALPHA,  %xmm3
	movl	K, %eax
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14

.L13:
	movapd	 0 * SIZE(BB), %xmm0
	movapd	 0 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movapd	 2 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm5
	movapd	 4 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm6
	mulpd	 6 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm7

	addl	$8 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	subl	$1, %eax
	jg	.L13
	ALIGN_4

.L14:
	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm5
	mulpd	%xmm3, %xmm6
	mulpd	%xmm3, %xmm7

	movsd	0 * SIZE(%esi), %xmm0
	movhpd	1 * SIZE(%esi), %xmm0
	movsd	2 * SIZE(%esi), %xmm1
	movhpd	3 * SIZE(%esi), %xmm1
	movsd	4 * SIZE(%esi), %xmm2
	movhpd	5 * SIZE(%esi), %xmm2
	movsd	6 * SIZE(%esi), %xmm3
	movhpd	7 * SIZE(%esi), %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm5
	addpd	%xmm2, %xmm6
	addpd	%xmm3, %xmm7

	movsd	%xmm4, 0 * SIZE(%esi)
	movhpd	%xmm4, 1 * SIZE(%esi)
	movsd	%xmm5, 2 * SIZE(%esi)
	movhpd	%xmm5, 3 * SIZE(%esi)
	movsd	%xmm6, 4 * SIZE(%esi)
	movhpd	%xmm6, 5 * SIZE(%esi)
	movsd	%xmm7, 6 * SIZE(%esi)
	movhpd	%xmm7, 7 * SIZE(%esi)

	addl	$8 * SIZE, %esi		# coffset += 4
	BRANCH
	decl	%ebx			# i --
	jg	.L10
	ALIGN_2

.L20:
	movl	M,  %ebx
	testl	$4, %ebx
	jle	.L30

	leal	BUFFER, %ecx
	movl	K, %eax

	movapd	 0 * SIZE + BUFFER, %xmm2
	movapd	 0 * SIZE(%edx), %xmm0
	movapd	 8 * SIZE + BUFFER, %xmm3
	movapd	 8 * SIZE(%edx), %xmm1

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	sarl	$3, %eax
	je	.L22

.L21:
	movapd	 0 * SIZE(BB), %xmm0
	movapd	 0 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	mulpd	 2 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm5

	movapd	 2 * SIZE(BB), %xmm0
	movapd	 4 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	mulpd	 6 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm5

	movapd	 4 * SIZE(BB), %xmm0
	movapd	 8 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	mulpd	10 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm5

	movapd	 6 * SIZE(BB), %xmm0
	movapd	12 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	mulpd	14 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm5

	movapd	 8 * SIZE(BB), %xmm0
	movapd	16 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	mulpd	18 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm5

	movapd	10 * SIZE(BB), %xmm0
	movapd	20 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	mulpd	22 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm5

	movapd	12 * SIZE(BB), %xmm0
	movapd	24 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	mulpd	26 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm5

	movapd	14 * SIZE(BB), %xmm0
	movapd	28 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	mulpd	30 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm5

	addl   $32 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L21

.L22:
	movapd	ALPHA,  %xmm3
	movl	K, %eax
	andl	$7, %eax
	BRANCH
	je .L24

.L23:
	movapd	 0 * SIZE(BB), %xmm0
	movapd	 0 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	mulpd	 2 * SIZE(AA), %xmm0
	addpd	%xmm0, %xmm5

	addl	$4 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	subl	$1, %eax
	jg	.L23
	ALIGN_4

.L24:
	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm5

	movsd	0 * SIZE(%esi), %xmm0
	movhpd	1 * SIZE(%esi), %xmm0
	movsd	2 * SIZE(%esi), %xmm1
	movhpd	3 * SIZE(%esi), %xmm1

	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm5

	movsd	%xmm4, 0 * SIZE(%esi)
	movhpd	%xmm4, 1 * SIZE(%esi)
	movsd	%xmm5, 2 * SIZE(%esi)
	movhpd	%xmm5, 3 * SIZE(%esi)
	addl	$4 * SIZE, %esi		# coffset += 4
	ALIGN_4

.L30:
	movl	M,  %ebx
	testl	$2, %ebx
	jle	.L50

	leal	BUFFER, %ecx
	movl	K, %eax

	movapd	 0 * SIZE + BUFFER, %xmm2
	movapd	 0 * SIZE(AA), %xmm0
	movapd	 8 * SIZE + BUFFER, %xmm3
	movapd	 8 * SIZE(AA), %xmm1

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	sarl	$3, %eax
	je	.L32

.L31:
	movapd	 0 * SIZE(BB), %xmm0
	movapd	 0 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4

	movapd	 2 * SIZE(BB), %xmm0
	movapd	 2 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4

	movapd	 4 * SIZE(BB), %xmm0
	movapd	 4 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4

	movapd	 6 * SIZE(BB), %xmm0
	movapd	 6 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4

	movapd	 8 * SIZE(BB), %xmm0
	movapd	 8 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4

	movapd	10 * SIZE(BB), %xmm0
	movapd	10 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4

	movapd	12 * SIZE(BB), %xmm0
	movapd	12 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4

	movapd	14 * SIZE(BB), %xmm0
	movapd	14 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L31

.L32:
	movapd	ALPHA,  %xmm3
	movl	K, %eax
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L34

.L33:
	movapd	 0 * SIZE(BB), %xmm0
	movapd	 0 * SIZE(AA), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4

	addl	$2 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L33
	ALIGN_4

.L34:
	mulpd	%xmm3, %xmm4

	movsd	0 * SIZE(%esi), %xmm0
	movhpd	1 * SIZE(%esi), %xmm0

	addpd	%xmm0, %xmm4

	movsd	%xmm4, 0 * SIZE(%esi)
	movhpd	%xmm4, 1 * SIZE(%esi)
	addl	$2 * SIZE, %esi
	ALIGN_2

.L50:
	movl	M,  %ebx
	testl	$1, %ebx
	jle	.L99

	leal	BUFFER, %ecx
	movl	K, %eax

	movsd	 0 * SIZE + BUFFER, %xmm2
	movsd	 0 * SIZE(AA), %xmm0
	movsd	 8 * SIZE + BUFFER, %xmm3
	movsd	 4 * SIZE(AA), %xmm1

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	sarl	$3, %eax
	je	.L52

.L51:
	movsd	 0 * SIZE(AA), %xmm0
	mulsd	 0 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	movsd	 1 * SIZE(AA), %xmm0
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	movsd	 2 * SIZE(AA), %xmm0
	mulsd	 4 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	movsd	 3 * SIZE(AA), %xmm0
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	movsd	 4 * SIZE(AA), %xmm0
	mulsd	 8 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	movsd	 5 * SIZE(AA), %xmm0
	mulsd	10 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	movsd	 6 * SIZE(AA), %xmm0
	mulsd	12 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	movsd	 7 * SIZE(AA), %xmm0
	mulsd	14 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	addl   $ 8 * SIZE, AA
	addl   $16 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L51

.L52:
	movsd	ALPHA,  %xmm3
	movl	K, %eax
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L54

.L53:
	movsd	 0 * SIZE(AA), %xmm0
	mulsd	 0 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	addl	$1 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L53
	ALIGN_4

.L54:
	movsd	0 * SIZE(%esi), %xmm0
	mulsd	%xmm3, %xmm4
	addsd	%xmm0, %xmm4
	movsd	%xmm4, 0 * SIZE(%esi)
	ALIGN_2

.L99:
	addl	LDC, C
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
	ALIGN_2


	EPILOGUE
