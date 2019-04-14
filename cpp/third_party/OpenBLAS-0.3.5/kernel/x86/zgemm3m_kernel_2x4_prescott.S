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
#define ALPHA	16 + STACK + ARGS(%esp)
#define A	32 + STACK + ARGS(%esp)
#define ARG_B	36 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define ARG_LDC	44 + STACK + ARGS(%esp)
#define OFFSET	48 + STACK + ARGS(%esp)

#define J	 0 + STACK(%esp)
#define BX	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#if defined(PENTIUM4) || defined(PENTIUMM)
#define PREFETCH_R   (8 * 4)
#define PREFETCH     prefetcht1
#define PREFETCHSIZE 84
#endif

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi

#define KERNEL1(address) \
	mulpd	 %xmm0, %xmm2; \
	PREFETCH  (PREFETCHSIZE + 0) * SIZE + (address) * 1 * SIZE(AA); \
	addpd	 %xmm2, %xmm4; \
	movddup	 1 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm5; \
	movddup	 2 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	 3 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	movapd	 2 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	addpd	 %xmm2, %xmm7; \
	movddup	 4 * SIZE + (address) * 2 * SIZE(BB), %xmm2

#define KERNEL2(address) \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm4; \
	movddup	 5 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm5; \
	movddup	 6 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	 7 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	movapd	 4 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	addpd	 %xmm2, %xmm7; \
	movddup	16 * SIZE + (address) * 2 * SIZE(BB), %xmm2

#define KERNEL3(address) \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	 9 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm5; \
	movddup	10 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	11 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	movapd	 6 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	addpd	 %xmm3, %xmm7; \
	movddup	12 * SIZE + (address) * 2 * SIZE(BB), %xmm3

#define KERNEL4(address) \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	13 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm5; \
	movddup	14 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	15 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	movapd	16 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	addpd	 %xmm3, %xmm7; \
	movddup	24 * SIZE + (address) * 2 * SIZE(BB), %xmm3

#define KERNEL5(address) \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm4; \
	movddup	17 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm5; \
	movddup	18 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	19 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	movapd	10 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	addpd	 %xmm2, %xmm7

#define KERNEL6(address) \
	movddup	20 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm4; \
	movddup	21 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm5; \
	movddup	22 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	23 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	movapd	12 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	addpd	 %xmm2, %xmm7; \
	movddup	32 * SIZE + (address) * 2 * SIZE(BB), %xmm2

#define KERNEL7(address) \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	25 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm5; \
	movddup	26 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	27 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	movapd	14 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	addpd	 %xmm3, %xmm7; \
	movddup	28 * SIZE + (address) * 2 * SIZE(BB), %xmm3

#define KERNEL8(address) \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	29 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm5; \
	movddup	30 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	31 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	movapd	24 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	addpd	 %xmm3, %xmm7; \
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
	sarl	$2, %eax
	movl	%eax, J
	jle	.L30
	ALIGN_2

.L10:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sall	$BASE_SHIFT + 2, %eax
	leal	(B, %eax), %eax
	movl	%eax, BX

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

	movl	B, BB
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 4), BB
#endif

	movl	BX, %eax
	prefetcht2  0 * SIZE(%eax)
	subl	$-4 * SIZE, BX

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movddup	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movddup	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

	leal	(LDC, LDC, 2), %eax

#ifdef PENTIUM4
	prefetchnta	3 * SIZE(%esi)
	prefetchnta	3 * SIZE(%esi, LDC, 1)
	prefetchnta	3 * SIZE(%esi, LDC, 2)
	prefetchnta	3 * SIZE(%esi, %eax, 1)
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
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif

#ifdef CORE_PRESCOTT
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
#if 1
	cmpl	$128 *  8, %eax
	jle	.L12
	KERNEL1(16  *  8)
	KERNEL2(16  *  8)
	KERNEL3(16  *  8)
	KERNEL4(16  *  8)
	KERNEL5(16  *  8)
	KERNEL6(16  *  8)
	KERNEL7(16  *  8)
	KERNEL8(16  *  8)
	cmpl	$128 *  9, %eax
	jle	.L12
	KERNEL1(16  *  9)
	KERNEL2(16  *  9)
	KERNEL3(16  *  9)
	KERNEL4(16  *  9)
	KERNEL5(16  *  9)
	KERNEL6(16  *  9)
	KERNEL7(16  *  9)
	KERNEL8(16  *  9)
	cmpl	$128 * 10, %eax
	jle	.L12
	KERNEL1(16  * 10)
	KERNEL2(16  * 10)
	KERNEL3(16  * 10)
	KERNEL4(16  * 10)
	KERNEL5(16  * 10)
	KERNEL6(16  * 10)
	KERNEL7(16  * 10)
	KERNEL8(16  * 10)
	cmpl	$128 * 11, %eax
	jle	.L12
	KERNEL1(16  * 11)
	KERNEL2(16  * 11)
	KERNEL3(16  * 11)
	KERNEL4(16  * 11)
	KERNEL5(16  * 11)
	KERNEL6(16  * 11)
	KERNEL7(16  * 11)
	KERNEL8(16  * 11)
	cmpl	$128 * 12, %eax
	jle	.L12
	KERNEL1(16  * 12)
	KERNEL2(16  * 12)
	KERNEL3(16  * 12)
	KERNEL4(16  * 12)
	KERNEL5(16  * 12)
	KERNEL6(16  * 12)
	KERNEL7(16  * 12)
	KERNEL8(16  * 12)
	cmpl	$128 * 13, %eax
	jle	.L12
	KERNEL1(16  * 13)
	KERNEL2(16  * 13)
	KERNEL3(16  * 13)
	KERNEL4(16  * 13)
	KERNEL5(16  * 13)
	KERNEL6(16  * 13)
	KERNEL7(16  * 13)
	KERNEL8(16  * 13)
	cmpl	$128 * 14, %eax
	jle	.L12
	KERNEL1(16  * 14)
	KERNEL2(16  * 14)
	KERNEL3(16  * 14)
	KERNEL4(16  * 14)
	KERNEL5(16  * 14)
	KERNEL6(16  * 14)
	KERNEL7(16  * 14)
	KERNEL8(16  * 14)
	cmpl	$128 * 15, %eax
	jle	.L12
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

.L12:
	leal	(AA, %eax, 1), AA	# * 16
	leal	(BB, %eax, 2), BB	# * 64

#else

	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	mulpd	 %xmm0, %xmm2
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 5 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	 6 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 7 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 4 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	16 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	 9 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm5
	movddup	10 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	11 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	movapd	 6 * SIZE(AA), %xmm0
	addpd	 %xmm3, %xmm7
	movddup	12 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	13 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm5
	movddup	14 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	15 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	movapd	16 * SIZE(AA), %xmm0
	addpd	 %xmm3, %xmm7
	movddup	24 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	17 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	18 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	19 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	movapd	10 * SIZE(AA), %xmm1
	addpd	 %xmm2, %xmm7
	movddup	20 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	21 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	22 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	23 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	movapd	12 * SIZE(AA), %xmm1
	addpd	 %xmm2, %xmm7
	movddup	32 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	25 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm5
	movddup	26 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	27 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	14 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm7
	movddup	28 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	29 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm5
	movddup	30 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	31 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	24 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm7
	movddup	40 * SIZE(BB), %xmm3

	addl   $32 * SIZE, BB
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
	movsd	0 + ALPHA,  %xmm3
	movhps	8 + ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_3

.L16:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
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
	ALIGN_3

.L20:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L29


#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 4), BB
#endif

	movddup	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movddup	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(BB), %xmm3
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
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$4, %eax
	je	.L25
	ALIGN_4

.L22:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movddup	 1 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm6
	movapd	16 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm7
	movddup	 2 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	mulpd	10 * SIZE(BB), %xmm0
	addpd	 %xmm3, %xmm4
	movapd	12 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm5
	movddup	 3 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	mulpd	14 * SIZE(BB), %xmm0
	addpd	 %xmm3, %xmm6
	movapd	24 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm7
	movddup	 4 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	18 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm4
	movapd	20 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movddup	 5 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	22 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm6
	movapd	32 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm7
	movddup	 6 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	mulpd	26 * SIZE(BB), %xmm0
	addpd	 %xmm3, %xmm4
	movapd	28 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm5
	movddup	 7 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	mulpd	30 * SIZE(BB), %xmm0
	addpd	 %xmm3, %xmm6
	movapd	40 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm7
	movddup	16 * SIZE(AA), %xmm0
	mulpd	 %xmm1, %xmm2
	mulpd	34 * SIZE(BB), %xmm1
	addpd	 %xmm2, %xmm4
	movapd	36 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm5
	movddup	 9 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm2
	mulpd	38 * SIZE(BB), %xmm1
	addpd	 %xmm2, %xmm6
	movapd	48 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm7
	movddup	10 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	mulpd	42 * SIZE(BB), %xmm1
	addpd	 %xmm3, %xmm4
	movapd	44 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm5
	movddup	11 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	mulpd	46 * SIZE(BB), %xmm1
	addpd	 %xmm3, %xmm6
	movapd	56 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm7
	movddup	12 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm2
	mulpd	50 * SIZE(BB), %xmm1
	addpd	 %xmm2, %xmm4
	movapd	52 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm5
	movddup	13 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm2
	mulpd	54 * SIZE(BB), %xmm1
	addpd	 %xmm2, %xmm6
	movapd	64 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm7
	movddup	14 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	mulpd	58 * SIZE(BB), %xmm1
	addpd	 %xmm3, %xmm4
	movapd	60 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm5
	movddup	15 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	mulpd	62 * SIZE(BB), %xmm1
	addpd	 %xmm3, %xmm6
	movapd	72 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm7
	movddup	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movsd	0 + ALPHA,  %xmm3
	movhps	8 + ALPHA,  %xmm3
	andl	$15, %eax		# if (k & 1)
	BRANCH
	je .L28

.L26:
	mulpd	 %xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movddup	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB

	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	leal	(%esi, LDC, 1), %eax

	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	leal	(LDC, LDC, 2), %eax

	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0
	movsd	0 * SIZE(%esi, LDC), %xmm1
	movhps	1 * SIZE(%esi, LDC), %xmm1

	pshufd  $0x44,  %xmm4,  %xmm2
	unpckhpd %xmm4, %xmm4

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm4
	addpd	 %xmm4,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi)
	movhps	%xmm0, 1 * SIZE(%esi)
	movlps	%xmm1, 0 * SIZE(%esi, LDC)
	movhps	%xmm1, 1 * SIZE(%esi, LDC)

	movsd	0 * SIZE(%esi, LDC, 2), %xmm0
	movhps	1 * SIZE(%esi, LDC, 2), %xmm0
	movsd	0 * SIZE(%esi, %eax), %xmm1
	movhps	1 * SIZE(%esi, %eax), %xmm1

	pshufd  $0x44,  %xmm5,  %xmm2
	unpckhpd %xmm5, %xmm5

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm5
	addpd	 %xmm5,  %xmm1

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
	movl	BB, B
	addl	%eax, C			# c += 4 * ldc
	decl	J			# j --
	jg	.L10
	ALIGN_4

.L30:
	testl	$2, N
	je	.L60

	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L50
	ALIGN_4

.L41:
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

#ifdef HAVE_3DNOW
	prefetchw 2 * SIZE(%esi)
	prefetchw 2 * SIZE(%esi, LDC)
#endif

#ifdef PENTIUM4
	prefetchnta	3 * SIZE(%esi)
	prefetchnta	3 * SIZE(%esi, LDC)
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
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 4 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 5 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 6 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm5
	movddup	 6 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 7 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	16 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	16 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	 9 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	10 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm5
	movddup	10 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	11 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	12 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm7
	movddup	12 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	13 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	14 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm5
	movddup	14 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	15 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	24 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm7
	movddup	24 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L42
	ALIGN_4

.L45:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movsd	0 + ALPHA,  %xmm3
	movhps	8 + ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L48
	ALIGN_3

.L46:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
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

	movl	B, BB
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), BB
#endif

	movddup	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movddup	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(BB), %xmm3
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
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$4, %eax
	je	.L55
	ALIGN_4

.L52:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	movddup	 1 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm4
	mulpd	 2 * SIZE(BB), %xmm0
	movapd	16 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movddup	 2 * SIZE(AA), %xmm0
	mulpd	 4 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm6
	movddup	 3 * SIZE(AA), %xmm0
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm7
	movddup	 4 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	movddup	 5 * SIZE(AA), %xmm0
	addpd	 %xmm3, %xmm4
	mulpd	10 * SIZE(BB), %xmm0
	movapd	24 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm5
	movddup	 6 * SIZE(AA), %xmm0
	mulpd	12 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm6
	movddup	 7 * SIZE(AA), %xmm0
	mulpd	14 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm7
	movddup	16 * SIZE(AA), %xmm0
	mulpd	 %xmm1, %xmm2
	movddup	 9 * SIZE(AA), %xmm1
	addpd	 %xmm2, %xmm4
	mulpd	18 * SIZE(BB), %xmm1
	movapd	32 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm5
	movddup	10 * SIZE(AA), %xmm1
	mulpd	20 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm6
	movddup	11 * SIZE(AA), %xmm1
	mulpd	22 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm7
	movddup	12 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	movddup	13 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm4
	mulpd	26 * SIZE(BB), %xmm1
	movapd	40 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm5
	movddup	14 * SIZE(AA), %xmm1
	mulpd	28 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm6
	movddup	15 * SIZE(AA), %xmm1
	mulpd	30 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm7
	movddup	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
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
	movsd	0 + ALPHA, %xmm3
	movhps	8 + ALPHA, %xmm3
	andl	$15, %eax		# if (k & 1)
	BRANCH
	je .L58

.L56:
	mulpd	 %xmm0, %xmm2
	movddup	 1 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm4
	movapd	 2 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
	addpd	%xmm6, %xmm4

	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0
	movsd	0 * SIZE(%esi, LDC), %xmm1
	movhps	1 * SIZE(%esi, LDC), %xmm1

	pshufd  $0x44,  %xmm4,  %xmm2
	unpckhpd %xmm4, %xmm4

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm4
	addpd	 %xmm4,  %xmm1

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
	movl	BB, B
	addl	%eax, C			# c += 4 * ldc
	ALIGN_4

.L60:
	testl	$1, N
	je	.L999

	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L80
	ALIGN_4

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), BB
#endif

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movddup	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movddup	 4 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#ifdef PENTIUM4
	prefetchnta 3 * SIZE(%esi)
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
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm2, %xmm0
	movddup	 1 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm4
	movapd	16 * SIZE(AA), %xmm0
	mulpd	 2 * SIZE(AA), %xmm2
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 4 * SIZE(AA), %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 6 * SIZE(AA), %xmm2
	addpd	 %xmm2, %xmm7
	movddup	 8 * SIZE(BB), %xmm2
	mulpd	 %xmm3, %xmm1
	movddup	 5 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm4
	movapd	24 * SIZE(AA), %xmm1
	mulpd	10 * SIZE(AA), %xmm3
	addpd	 %xmm3, %xmm5
	movddup	 6 * SIZE(BB), %xmm3
	mulpd	12 * SIZE(AA), %xmm3
	addpd	 %xmm3, %xmm6
	movddup	 7 * SIZE(BB), %xmm3
	mulpd	14 * SIZE(AA), %xmm3
	addpd	 %xmm3, %xmm7
	movddup	12 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $ 8 * SIZE, BB
	decl   %eax
	jne    .L72
	ALIGN_4

.L75:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movsd	0 + ALPHA, %xmm3
	movhps	8 + ALPHA, %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_3

.L76:
	mulpd	 %xmm2, %xmm0
	movddup	 1 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm4
	movapd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
	addpd	%xmm6, %xmm4

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

	movl	B, BB
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), BB
#endif

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(BB), %xmm3
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
	sarl	$4, %eax
	je	.L85
	ALIGN_4

.L82:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm4
	mulpd	 2 * SIZE(BB), %xmm0
	movapd	16 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	 4 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm6
	movapd	 6 * SIZE(AA), %xmm0
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm7
	movapd	16 * SIZE(AA), %xmm0
	mulpd	 %xmm1, %xmm3
	movapd	10 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm4
	mulpd	10 * SIZE(BB), %xmm1
	movapd	24 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm5
	movapd	12 * SIZE(AA), %xmm1
	mulpd	12 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm6
	movapd	14 * SIZE(AA), %xmm1
	mulpd	14 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm7
	movapd	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
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
	movsd	0 + ALPHA, %xmm3
	movhps	8 + ALPHA, %xmm3
	andl	$15, %eax		# if (k & 1)
	BRANCH
	je .L88

.L86:
	mulsd	 %xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	addsd	 %xmm2, %xmm4
	movsd	 1 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L86
	ALIGN_4

.L88:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
	addpd	%xmm6, %xmm4

	haddpd	%xmm4, %xmm4

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
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
