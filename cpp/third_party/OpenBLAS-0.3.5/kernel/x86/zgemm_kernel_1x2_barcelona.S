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
#define OLD_B	36 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define OLD_LDC	44 + STACK + ARGS(%esp)
#define OFFSET	48 + STACK + ARGS(%esp)

#define J	 0 + STACK(%esp)
#define BX	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#define B	%edi
#define LDC	%ebp
#define AO	%edx
#define BO	%ecx
#define CO	%esi
#define	I	%ebx

#define movsd  movlps
#define movapd movups
#define movlpd movlps
#define movhpd movhps

#define PREFETCH     prefetch
#define PREFETCHSIZE  (8 *  7 + 0)

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	addpd
#define ADD2	addpd
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	addpd
#define ADD2	subpd
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	subpd
#define ADD2	addpd
#else
#define ADD1	subpd
#define ADD2	subpd
#endif

#define KERNEL1(address) \
	mulpd	%xmm0, %xmm1; \
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO, %eax, 2); \
	mulpd	-14 * SIZE(BO, %eax, 4), %xmm0; \
	ADD1	%xmm1, %xmm4; \
	movapd	-12 * SIZE(BO, %eax, 4), %xmm1; \
	ADD1	%xmm0, %xmm6; \
	movddup	-15 * SIZE(AO, %eax, 2), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	-14 * SIZE(BO, %eax, 4), %xmm0; \
	ADD2	%xmm0, %xmm7; \
	movddup	-14 * SIZE(AO, %eax, 2), %xmm0

#define KERNEL2(address) \
	ADD2	%xmm2, %xmm5; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm0, %xmm1; \
	mulpd	-10 * SIZE(BO, %eax, 4), %xmm0; \
	ADD1	%xmm1, %xmm4; \
	movapd	 -8 * SIZE(BO, %eax, 4), %xmm1; \
	ADD1	%xmm0, %xmm6; \
	movddup	-13 * SIZE(AO, %eax, 2), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	-10 * SIZE(BO, %eax, 4), %xmm0; \
	ADD2	%xmm0, %xmm7; \
	movddup	-12 * SIZE(AO, %eax, 2), %xmm0

#define KERNEL3(address) \
	ADD2	%xmm2, %xmm5; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm0, %xmm1; \
	mulpd	 -6 * SIZE(BO, %eax, 4), %xmm0; \
	ADD1	%xmm1, %xmm4; \
	movapd	 -4 * SIZE(BO, %eax, 4), %xmm1; \
	ADD1	%xmm0, %xmm6; \
	movddup	-11 * SIZE(AO, %eax, 2), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 -6 * SIZE(BO, %eax, 4), %xmm0; \
	ADD2	%xmm0, %xmm7; \
	movddup	-10 * SIZE(AO, %eax, 2), %xmm0

#define KERNEL4(address) \
	ADD2	%xmm2, %xmm5; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm0, %xmm1; \
	mulpd	 -2 * SIZE(BO, %eax, 4), %xmm0; \
	ADD1	%xmm1, %xmm4; \
	movapd	          (BO, %eax, 4), %xmm1; \
	ADD1	%xmm0, %xmm6; \
	movddup	 -9 * SIZE(AO, %eax, 2), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 -2 * SIZE(BO, %eax, 4), %xmm0; \
	ADD2	%xmm0, %xmm7; \
	movddup	          (AO, %eax, 2), %xmm0

#define KERNEL5(address) \
	ADD2	%xmm2, %xmm5; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm3, %xmm1; \
	mulpd	  2 * SIZE(BO, %eax, 4), %xmm3; \
	ADD1	%xmm1, %xmm4; \
	movapd	  4 * SIZE(BO, %eax, 4), %xmm1; \
	ADD1	%xmm3, %xmm6; \
	movddup	 -7 * SIZE(AO, %eax, 2), %xmm3; \
	mulpd	%xmm3, %xmm2; \
	mulpd	  2 * SIZE(BO, %eax, 4), %xmm3; \
	ADD2	%xmm3, %xmm7; \
	movddup	 -6 * SIZE(AO, %eax, 2), %xmm3

#define KERNEL6(address) \
	ADD2	%xmm2, %xmm5; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm3, %xmm1; \
	mulpd	  6 * SIZE(BO, %eax, 4), %xmm3; \
	ADD1	%xmm1, %xmm4; \
	movapd	  8 * SIZE(BO, %eax, 4), %xmm1; \
	ADD1	%xmm3, %xmm6; \
	movddup	 -5 * SIZE(AO, %eax, 2), %xmm3; \
	mulpd	%xmm3, %xmm2; \
	mulpd	  6 * SIZE(BO, %eax, 4), %xmm3; \
	ADD2	%xmm3, %xmm7; \
	movddup	 -4 * SIZE(AO, %eax, 2), %xmm3

#define KERNEL7(address) \
	ADD2	%xmm2, %xmm5; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm3, %xmm1; \
	mulpd	 10 * SIZE(BO, %eax, 4), %xmm3; \
	ADD1	%xmm1, %xmm4; \
	movapd	 12 * SIZE(BO, %eax, 4), %xmm1; \
	ADD1	%xmm3, %xmm6; \
	movddup	 -3 * SIZE(AO, %eax, 2), %xmm3; \
	mulpd	%xmm3, %xmm2; \
	mulpd	 10 * SIZE(BO, %eax, 4), %xmm3; \
	ADD2	%xmm3, %xmm7; \
	movddup	 -2 * SIZE(AO, %eax, 2), %xmm3

#define KERNEL8(address) \
	ADD2	%xmm2, %xmm5; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm3, %xmm1; \
	mulpd	 14 * SIZE(BO, %eax, 4), %xmm3; \
	ADD1	%xmm1, %xmm4; \
	movapd	 16 * SIZE(BO, %eax, 4), %xmm1; \
	ADD1	%xmm3, %xmm6; \
	movddup	 -1 * SIZE(AO, %eax, 2), %xmm3; \
	mulpd	%xmm3, %xmm2; \
	mulpd	 14 * SIZE(BO, %eax, 4), %xmm3; \
	ADD2	%xmm3, %xmm7; \
	movddup	  8 * SIZE(AO, %eax, 2), %xmm3; \
	ADD2	%xmm2, %xmm5; \
	movapd	%xmm1, %xmm2

	PROLOGUE

	subl	$ARGS, %esp

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	OLD_B,   B
	movl	OLD_LDC, LDC

#ifdef TRMMKERNEL
	movl	OFFSET, %eax

#ifndef LEFT
	negl	%eax
#endif

	movl	%eax, KK
#endif

	subl	$-16 * SIZE, A
	subl	$-16 * SIZE, B

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

	leal	GEMM_DEFAULT_Q * GEMM_DEFAULT_UNROLL_N * SIZE(B), %eax
	movl	%eax, BX

	movl	C, CO
	movl	A, AO
	movl	M,  I
	testl	I,  I
	jle	.L100
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BO
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(B,  %eax, 4), BO
#endif

	movl	BX, %eax

	prefetcht2  0 * SIZE(%eax)

	subl	$-8 * SIZE, BX

	movddup	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm1
	pxor	%xmm4, %xmm4
	movddup	 -8 * SIZE(AO), %xmm3
	pxor	%xmm5, %xmm5

	prefetchw 1 * SIZE(CO)
	pxor	%xmm6, %xmm6
	prefetchw 1 * SIZE(CO, LDC)
	pxor	%xmm7, %xmm7
	movapd	%xmm1, %xmm2

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

	andl	$-8, %eax

	leal	(, %eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 4), BO
	negl	%eax
	NOBRANCH
	je	.L15
	ALIGN_3

.L12:
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	addl	$8 * SIZE, %eax
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
	addl	$8 * SIZE, %eax
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
	addl	$8 * SIZE, %eax
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
	addl	$8 * SIZE, %eax
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
	addl	$8 * SIZE, %eax
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
	addl	$8 * SIZE, %eax
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
	addl	$8 * SIZE, %eax
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
	addl	$8 * SIZE, %eax
	BRANCH
	jl	.L12
	ALIGN_3

.L15:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14

	leal	(, %eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 4), BO
	negl	%eax
	ALIGN_4

.L16:
	mulpd	%xmm0, %xmm1
	mulpd	-14 * SIZE(BO, %eax, 4), %xmm0
	ADD1	%xmm1, %xmm4
	movapd	-12 * SIZE(BO, %eax, 4), %xmm1
	ADD1	%xmm0, %xmm6
	movddup	-15 * SIZE(AO, %eax, 2), %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	-14 * SIZE(BO, %eax, 4), %xmm0
	ADD2	%xmm0, %xmm7
	movddup	-14 * SIZE(AO, %eax, 2), %xmm0
	ADD2	%xmm2, %xmm5
	movapd	%xmm1, %xmm2

	addl	$SIZE, %eax
	jl	.L16
	ALIGN_4

.L14:
#ifndef TRMMKERNEL
	movupd	0 * SIZE(CO), %xmm0
	movupd	0 * SIZE(CO, LDC), %xmm1
#endif

	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	addsubpd %xmm5,  %xmm4
	addsubpd %xmm7,  %xmm6

	pshufd	$0x4e, %xmm4, %xmm5
	pshufd	$0x4e, %xmm6, %xmm7
#else
	addsubpd %xmm4,  %xmm5
	addsubpd %xmm6,  %xmm7

	movapd	 %xmm5,  %xmm4
	pshufd	$0x4e, %xmm5,  %xmm5
	movapd	 %xmm7,  %xmm6
	pshufd	$0x4e, %xmm7,  %xmm7
#endif

	mulpd	%xmm2, %xmm4
	mulpd	%xmm3, %xmm5
	mulpd	%xmm2, %xmm6
	mulpd	%xmm3, %xmm7

	addsubpd %xmm5, %xmm4
	addsubpd %xmm7, %xmm6

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm6
#endif

	movlpd	%xmm4, 0 * SIZE(CO)
	movhpd	%xmm4, 1 * SIZE(CO)
	movlpd	%xmm6, 0 * SIZE(CO, LDC)
	movhpd	%xmm6, 1 * SIZE(CO, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, CO		# coffset += 4
	decl	I			# i --
	jg	.L10
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	movl	BO, B

	leal	(, LDC, 2), %eax
	addl	%eax, C			# c += ldc
	decl	J			# j --
	jg	.L01
	ALIGN_4

.L100:
	movl	N, %eax
	andl	$1, %eax
	jle	.L500
	ALIGN_4

.L101:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	C, CO
	movl	A, AO

	movl	M,    I
	testl	%ebx, I
	jle	.L500
	ALIGN_4

.L110:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BO
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(B,  %eax, 2), BO
#endif

	movddup	-16 * SIZE(AO), %xmm0
	pxor	%xmm4, %xmm4
	movddup	-15 * SIZE(AO), %xmm1
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	prefetchw 1 * SIZE(CO)

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
	je	.L112
	ALIGN_4

.L111:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AO)

	mulpd	-16 * SIZE(BO), %xmm0
	ADD1	%xmm0, %xmm4
	movddup	-14 * SIZE(AO), %xmm0
	mulpd	-16 * SIZE(BO), %xmm1
	ADD2	%xmm1, %xmm5
	movddup	-13 * SIZE(AO), %xmm1

	mulpd	-14 * SIZE(BO), %xmm0
	ADD1	%xmm0, %xmm6
	movddup	-12 * SIZE(AO), %xmm0
	mulpd	-14 * SIZE(BO), %xmm1
	ADD2	%xmm1, %xmm7
	movddup	-11 * SIZE(AO), %xmm1

	mulpd	-12 * SIZE(BO), %xmm0
	ADD1	%xmm0, %xmm4
	movddup	-10 * SIZE(AO), %xmm0
	mulpd	-12 * SIZE(BO), %xmm1
	ADD2	%xmm1, %xmm5
	movddup	 -9 * SIZE(AO), %xmm1

	mulpd	-10 * SIZE(BO), %xmm0
	ADD1	%xmm0, %xmm6
	movddup	 -8 * SIZE(AO), %xmm0
	mulpd	-10 * SIZE(BO), %xmm1
	ADD2	%xmm1, %xmm7
	movddup	 -7 * SIZE(AO), %xmm1

	mulpd	 -8 * SIZE(BO), %xmm0
	ADD1	%xmm0, %xmm4
	movddup	 -6 * SIZE(AO), %xmm0
	mulpd	 -8 * SIZE(BO), %xmm1
	ADD2	%xmm1, %xmm5
	movddup	 -5 * SIZE(AO), %xmm1

	mulpd	 -6 * SIZE(BO), %xmm0
	ADD1	%xmm0, %xmm6
	movddup	 -4 * SIZE(AO), %xmm0
	mulpd	 -6 * SIZE(BO), %xmm1
	ADD2	%xmm1, %xmm7
	movddup	 -3 * SIZE(AO), %xmm1

	mulpd	 -4 * SIZE(BO), %xmm0
	ADD1	%xmm0, %xmm4
	movddup	 -2 * SIZE(AO), %xmm0
	mulpd	 -4 * SIZE(BO), %xmm1
	ADD2	%xmm1, %xmm5
	movddup	 -1 * SIZE(AO), %xmm1

	mulpd	 -2 * SIZE(BO), %xmm0
	ADD1	%xmm0, %xmm6
	movddup	  0 * SIZE(AO), %xmm0
	mulpd	 -2 * SIZE(BO), %xmm1
	ADD2	%xmm1, %xmm7
	movddup	  1 * SIZE(AO), %xmm1

	subl   $-16 * SIZE, AO
	subl   $-16 * SIZE, BO
	decl   %eax
	jne    .L111
	ALIGN_4

.L112:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L114
	ALIGN_4

.L113:
	mulpd	-16 * SIZE(BO), %xmm0
	ADD1	%xmm0, %xmm4
	movddup	-14 * SIZE(AO), %xmm0
	mulpd	-16 * SIZE(BO), %xmm1
	ADD2	%xmm1, %xmm5
	movddup	-13 * SIZE(AO), %xmm1

	addl	$2 * SIZE, AO
	addl	$2 * SIZE, BO
	decl	%eax
	jg	.L113
	ALIGN_4

.L114:
#ifndef TRMMKERNEL
	movupd	0 * SIZE(CO), %xmm0
#endif

	movddup	ALPHA_R, %xmm2
	movddup ALPHA_I, %xmm3

	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	SHUFPD_1 %xmm5, %xmm5

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	addsubpd %xmm5,  %xmm4
	pshufd	$0x4e, %xmm4, %xmm5
#else
	addsubpd %xmm4,  %xmm5
	movapd	 %xmm5,  %xmm4
	pshufd	$0x4e, %xmm5,  %xmm5
#endif

	mulpd	%xmm2, %xmm4
	mulpd	%xmm3, %xmm5

	addsubpd %xmm5, %xmm4

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm4
#endif

	movlpd	%xmm4, 0 * SIZE(CO)
	movhpd	%xmm4, 1 * SIZE(CO)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, CO		# coffset += 4
	decl	I			# i --
	jg	.L110
	ALIGN_4

.L500:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp

	ret

	EPILOGUE
