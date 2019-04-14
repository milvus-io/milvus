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
#define ARGS	16

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	24 + STACK + ARGS(%esp)
#define A	32 + STACK + ARGS(%esp)
#define ARG_B	36 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define ARG_LDC	44 + STACK + ARGS(%esp)
#define OFFSET	48 + STACK + ARGS(%esp)

#define J	 0 + STACK(%esp)
#define BX	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#ifdef PENTIUM4
#define PREFETCH_R   (8 * 4)
#define PREFETCH	prefetcht1
#define PREFETCHSIZE 84
#endif

#ifdef PENTIUMM
#define PREFETCH_R   (8 * 4)
#define PREFETCH	prefetcht1
#define PREFETCHSIZE 84
#endif

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
      defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADDSUB	  addpd
#else
#define ADDSUB	  subpd
#endif

#define KERNEL1(address) \
	mulpd	 %xmm0, %xmm2; \
	PREFETCH  (PREFETCHSIZE + 0) * SIZE + (address) * 1 * SIZE(AA); \
	addpd	 %xmm2, %xmm4; \
	movddup	 1 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	ADDSUB	 %xmm2, %xmm5; \
	movddup	 2 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	 3 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	movapd	 2 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	ADDSUB	 %xmm2, %xmm7; \
	movddup	 4 * SIZE + (address) * 2 * SIZE(BB), %xmm2

#define KERNEL2(address) \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm4; \
	movddup	 5 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	ADDSUB	 %xmm2, %xmm5; \
	movddup	 6 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	 7 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	movapd	 4 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	ADDSUB	 %xmm2, %xmm7; \
	movddup	16 * SIZE + (address) * 2 * SIZE(BB), %xmm2

#define KERNEL3(address) \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	 9 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	ADDSUB	 %xmm3, %xmm5; \
	movddup	10 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	11 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	movapd	 6 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	ADDSUB	 %xmm3, %xmm7; \
	movddup	12 * SIZE + (address) * 2 * SIZE(BB), %xmm3

#define KERNEL4(address) \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	13 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	ADDSUB	 %xmm3, %xmm5; \
	movddup	14 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	15 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	movapd	16 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	ADDSUB	 %xmm3, %xmm7; \
	movddup	24 * SIZE + (address) * 2 * SIZE(BB), %xmm3

#define KERNEL5(address) \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm4; \
	movddup	17 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	ADDSUB	 %xmm2, %xmm5; \
	movddup	18 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	19 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	movapd	10 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	ADDSUB	 %xmm2, %xmm7; \
	movddup	20 * SIZE + (address) * 2 * SIZE(BB), %xmm2

#define KERNEL6(address) \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm4; \
	movddup	21 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	ADDSUB	 %xmm2, %xmm5; \
	movddup	22 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	23 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	movapd	12 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	ADDSUB	 %xmm2, %xmm7

#define KERNEL7(address) \
	movddup	32 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	25 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	ADDSUB	 %xmm3, %xmm5; \
	movddup	26 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	27 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	movapd	14 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	ADDSUB	 %xmm3, %xmm7; \
	movddup	28 * SIZE + (address) * 2 * SIZE(BB), %xmm3

#define KERNEL8(address) \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	29 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	ADDSUB	 %xmm3, %xmm5; \
	movddup	30 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	31 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	movapd	24 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	ADDSUB	 %xmm3, %xmm7; \
	movddup	40 * SIZE + (address) * 2 * SIZE(BB), %xmm3

	PROLOGUE

	subl	$ARGS, %esp

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	ARG_B,   B
	movl	ARG_LDC, LDC

#ifdef TRMMKERNEL
	movl	OFFSET, %eax
#ifndef LEFT
	negl	%eax
#endif
	movl	%eax, KK
#endif

	sall	$ZBASE_SHIFT, LDC

	movl	N,  %eax
	sarl	$1, %eax
	movl	%eax, J			# j = n
	jle	.L100
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	B, BX

	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a

	movl	M,  %ebx
	testl	%ebx, %ebx
	jle	.L100
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 4), BB
#endif

	movl	BX, %eax

	prefetcht2  (PREFETCH_R +  0) * SIZE(%eax)
	prefetcht2  (PREFETCH_R + 16) * SIZE(%eax)

	subl	$-8 * SIZE, BX

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movddup	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movddup	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#ifdef PENTIUM4
	prefetchnta 3 * SIZE(%esi)
	prefetchnta 3 * SIZE(%esi, LDC)
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

#ifdef CORE_PRESCOTT
	andl	$-8, %eax
	sall	$4, %eax
	je	.L12

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
	jle	.L11
	KERNEL1(16  *  1)
	KERNEL2(16  *  1)
	KERNEL3(16  *  1)
	KERNEL4(16  *  1)
	KERNEL5(16  *  1)
	KERNEL6(16  *  1)
	KERNEL7(16  *  1)
	KERNEL8(16  *  1)
	cmpl	$128 *  2, %eax
	jle	.L11
	KERNEL1(16  *  2)
	KERNEL2(16  *  2)
	KERNEL3(16  *  2)
	KERNEL4(16  *  2)
	KERNEL5(16  *  2)
	KERNEL6(16  *  2)
	KERNEL7(16  *  2)
	KERNEL8(16  *  2)
	cmpl	$128 *  3, %eax
	jle	.L11
	KERNEL1(16  *  3)
	KERNEL2(16  *  3)
	KERNEL3(16  *  3)
	KERNEL4(16  *  3)
	KERNEL5(16  *  3)
	KERNEL6(16  *  3)
	KERNEL7(16  *  3)
	KERNEL8(16  *  3)
	cmpl	$128 *  4, %eax
	jle	.L11
	KERNEL1(16  *  4)
	KERNEL2(16  *  4)
	KERNEL3(16  *  4)
	KERNEL4(16  *  4)
	KERNEL5(16  *  4)
	KERNEL6(16  *  4)
	KERNEL7(16  *  4)
	KERNEL8(16  *  4)
	cmpl	$128 *  5, %eax
	jle	.L11
	KERNEL1(16  *  5)
	KERNEL2(16  *  5)
	KERNEL3(16  *  5)
	KERNEL4(16  *  5)
	KERNEL5(16  *  5)
	KERNEL6(16  *  5)
	KERNEL7(16  *  5)
	KERNEL8(16  *  5)
	cmpl	$128 *  6, %eax
	jle	.L11
	KERNEL1(16  *  6)
	KERNEL2(16  *  6)
	KERNEL3(16  *  6)
	KERNEL4(16  *  6)
	KERNEL5(16  *  6)
	KERNEL6(16  *  6)
	KERNEL7(16  *  6)
	KERNEL8(16  *  6)
	cmpl	$128 *  7, %eax
	jle	.L11
	KERNEL1(16  *  7)
	KERNEL2(16  *  7)
	KERNEL3(16  *  7)
	KERNEL4(16  *  7)
	KERNEL5(16  *  7)
	KERNEL6(16  *  7)
	KERNEL7(16  *  7)
	KERNEL8(16  *  7)
#if 1
	cmpl	$128 *  8, %eax
	jle	.L11
	KERNEL1(16  *  8)
	KERNEL2(16  *  8)
	KERNEL3(16  *  8)
	KERNEL4(16  *  8)
	KERNEL5(16  *  8)
	KERNEL6(16  *  8)
	KERNEL7(16  *  8)
	KERNEL8(16  *  8)
	cmpl	$128 *  9, %eax
	jle	.L11
	KERNEL1(16  *  9)
	KERNEL2(16  *  9)
	KERNEL3(16  *  9)
	KERNEL4(16  *  9)
	KERNEL5(16  *  9)
	KERNEL6(16  *  9)
	KERNEL7(16  *  9)
	KERNEL8(16  *  9)
	cmpl	$128 * 10, %eax
	jle	.L11
	KERNEL1(16  * 10)
	KERNEL2(16  * 10)
	KERNEL3(16  * 10)
	KERNEL4(16  * 10)
	KERNEL5(16  * 10)
	KERNEL6(16  * 10)
	KERNEL7(16  * 10)
	KERNEL8(16  * 10)
	cmpl	$128 * 11, %eax
	jle	.L11
	KERNEL1(16  * 11)
	KERNEL2(16  * 11)
	KERNEL3(16  * 11)
	KERNEL4(16  * 11)
	KERNEL5(16  * 11)
	KERNEL6(16  * 11)
	KERNEL7(16  * 11)
	KERNEL8(16  * 11)
	cmpl	$128 * 12, %eax
	jle	.L11
	KERNEL1(16  * 12)
	KERNEL2(16  * 12)
	KERNEL3(16  * 12)
	KERNEL4(16  * 12)
	KERNEL5(16  * 12)
	KERNEL6(16  * 12)
	KERNEL7(16  * 12)
	KERNEL8(16  * 12)
	cmpl	$128 * 13, %eax
	jle	.L11
	KERNEL1(16  * 13)
	KERNEL2(16  * 13)
	KERNEL3(16  * 13)
	KERNEL4(16  * 13)
	KERNEL5(16  * 13)
	KERNEL6(16  * 13)
	KERNEL7(16  * 13)
	KERNEL8(16  * 13)
	cmpl	$128 * 14, %eax
	jle	.L11
	KERNEL1(16  * 14)
	KERNEL2(16  * 14)
	KERNEL3(16  * 14)
	KERNEL4(16  * 14)
	KERNEL5(16  * 14)
	KERNEL6(16  * 14)
	KERNEL7(16  * 14)
	KERNEL8(16  * 14)
	cmpl	$128 * 15, %eax
	jle	.L11
	KERNEL1(16  * 15)
	KERNEL2(16  * 15)
	KERNEL3(16  * 15)
	KERNEL4(16  * 15)
	KERNEL5(16  * 15)
	KERNEL6(16  * 15)
	KERNEL7(16  * 15)
	KERNEL8(16  * 15)
#else
	addl	$32 * 4  * SIZE, AA
	addl	$32 * 8  * SIZE, BB
	subl	$128 * 8, %eax
	jg	.L1X
#endif

.L11:
	leal	(AA, %eax, 1), AA	# * 16
	leal	(BB, %eax, 2), BB	# * 64

#else

	sarl	$3, %eax
	je	.L12
	ALIGN_4

.L11:
	KERNEL1(16  *  0)
	KERNEL2(16  *  0)
	KERNEL3(16  *  0)
	KERNEL4(16  *  0)
	KERNEL5(16  *  0)
	KERNEL6(16  *  0)
	KERNEL7(16  *  0)
	KERNEL8(16  *  0)

	addl   $32 * SIZE, BB
	addl   $16 * SIZE, AA
	decl   %eax
	jne    .L11
	ALIGN_4
#endif

.L12:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movddup	ALPHA_R, %xmm1
	movddup	ALPHA_I, %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14
	ALIGN_4

.L13:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	ADDSUB	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L13
	ALIGN_4

.L14:
	pcmpeqb	%xmm0, %xmm0
	SHUFPD_1 %xmm5, %xmm5
	psllq	$63,   %xmm0
	SHUFPD_1 %xmm7, %xmm7

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	shufps	$0x04, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm4
	pxor	%xmm0, %xmm6
#else
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm4
	pxor	%xmm0, %xmm6
#endif

	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6

	movaps	%xmm4, %xmm5
	movaps	%xmm6, %xmm7

	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

	mulpd	%xmm1, %xmm4
	mulpd	%xmm1, %xmm6

	mulpd	%xmm3, %xmm5
	mulpd	%xmm3, %xmm7

	addsubpd	%xmm5, %xmm4
	addsubpd	%xmm7, %xmm6

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhpd	1 * SIZE(%esi), %xmm0
	movsd	0 * SIZE(%esi, LDC), %xmm2
	movhpd	1 * SIZE(%esi, LDC), %xmm2

	addpd	%xmm0, %xmm4
	addpd	%xmm2, %xmm6
#endif

	movsd	%xmm4, 0 * SIZE(%esi)
	movhpd	%xmm4, 1 * SIZE(%esi)
	movsd	%xmm6, 0 * SIZE(%esi, LDC)
	movhpd	%xmm6, 1 * SIZE(%esi, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, %esi		# coffset += 4
	decl	%ebx			# i --
	jg	.L10
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leal	(, LDC, 2), %eax
	movl	BB, B
	addl	%eax, C			# c += ldc
	decl	J			# j --
	jg	.L01
	ALIGN_4

.L100:
	movl	N, %eax
	testl	$1, %eax
	jle	.L500

	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	M,  %ebx
	testl	%ebx, %ebx
	jle	.L500
	ALIGN_4

L110:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 2), BB
#endif

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movddup	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movddup	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#ifdef PENTIUM4
	prefetchnta	4 * SIZE(%esi)
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
	je	L112
	ALIGN_4

L111:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 4 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 5 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 6 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm5
	movddup	 6 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 7 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	16 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm7
	movddup	16 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	 9 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	10 * SIZE(AA), %xmm1
	ADDSUB	 %xmm3, %xmm5
	movddup	10 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	11 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	12 * SIZE(AA), %xmm1
	ADDSUB	 %xmm3, %xmm7
	movddup	12 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	13 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	14 * SIZE(AA), %xmm1
	ADDSUB	 %xmm3, %xmm5
	movddup	14 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	15 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	24 * SIZE(AA), %xmm1
	ADDSUB	 %xmm3, %xmm7
	movddup	24 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    L111
	ALIGN_4

L112:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movddup	ALPHA_R, %xmm1
	movddup	ALPHA_I, %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je L114
	ALIGN_4

L113:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	L113
	ALIGN_4

L114:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	pcmpeqb	%xmm0, %xmm0
	SHUFPD_1 %xmm5, %xmm5
	psllq	$63,   %xmm0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	shufps	$0x04, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm4
#else
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm4
#endif

	addpd	%xmm5, %xmm4

	movaps	%xmm4, %xmm5

	SHUFPD_1 %xmm5, %xmm5

	mulpd	%xmm1, %xmm4
	mulpd	%xmm3, %xmm5

	addsubpd	%xmm5, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhpd	1 * SIZE(%esi), %xmm0

	addpd	%xmm0, %xmm4
#endif

	movsd	%xmm4, 0 * SIZE(%esi)
	movhpd	%xmm4, 1 * SIZE(%esi)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, %esi		# coffset += 4
	decl	%ebx			# i --
	jg	L110
	ALIGN_4

.L500:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
