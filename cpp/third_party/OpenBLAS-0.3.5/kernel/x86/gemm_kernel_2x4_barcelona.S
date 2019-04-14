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
#define A	24 + STACK + ARGS(%esp)
#define OLD_B	28 + STACK + ARGS(%esp)
#define C	32 + STACK + ARGS(%esp)
#define OLD_LDC	36 + STACK + ARGS(%esp)
#define OFFSET	40 + STACK + ARGS(%esp)

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

#define KERNEL1(address) \
	mulpd	%xmm0, %xmm1; \
	mulpd	-14 * SIZE(BO, %eax, 4), %xmm0; \
	addpd	%xmm1, %xmm4; \
	movapd	-12 * SIZE(BO, %eax, 4), %xmm1; \
	addpd	%xmm0, %xmm5; \
	movddup	-15 * SIZE(AO, %eax, 2), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	-14 * SIZE(BO, %eax, 4), %xmm0; \
	addpd	%xmm0, %xmm7; \
	movddup	-14 * SIZE(AO, %eax, 2), %xmm0

#define KERNEL2(address) \
	addpd	%xmm2, %xmm6; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm0, %xmm1; \
	mulpd	-10 * SIZE(BO, %eax, 4), %xmm0; \
	addpd	%xmm1, %xmm4; \
	movapd	 -8 * SIZE(BO, %eax, 4), %xmm1; \
	addpd	%xmm0, %xmm5; \
	movddup	-13 * SIZE(AO, %eax, 2), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	-10 * SIZE(BO, %eax, 4), %xmm0; \
	addpd	%xmm0, %xmm7; \
	movddup	-12 * SIZE(AO, %eax, 2), %xmm0

#define KERNEL3(address) \
	addpd	%xmm2, %xmm6; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm0, %xmm1; \
	mulpd	 -6 * SIZE(BO, %eax, 4), %xmm0; \
	addpd	%xmm1, %xmm4; \
	movapd	 -4 * SIZE(BO, %eax, 4), %xmm1; \
	addpd	%xmm0, %xmm5; \
	movddup	-11 * SIZE(AO, %eax, 2), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 -6 * SIZE(BO, %eax, 4), %xmm0; \
	addpd	%xmm0, %xmm7; \
	movddup	-10 * SIZE(AO, %eax, 2), %xmm0

#define KERNEL4(address) \
	addpd	%xmm2, %xmm6; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm0, %xmm1; \
	mulpd	 -2 * SIZE(BO, %eax, 4), %xmm0; \
	addpd	%xmm1, %xmm4; \
	movapd	          (BO, %eax, 4), %xmm1; \
	addpd	%xmm0, %xmm5; \
	movddup	 -9 * SIZE(AO, %eax, 2), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 -2 * SIZE(BO, %eax, 4), %xmm0; \
	addpd	%xmm0, %xmm7; \
	movddup	          (AO, %eax, 2), %xmm0

#define KERNEL5(address) \
	addpd	%xmm2, %xmm6; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm3, %xmm1; \
	mulpd	  2 * SIZE(BO, %eax, 4), %xmm3; \
	addpd	%xmm1, %xmm4; \
	movapd	  4 * SIZE(BO, %eax, 4), %xmm1; \
	addpd	%xmm3, %xmm5; \
	movddup	 -7 * SIZE(AO, %eax, 2), %xmm3; \
	mulpd	%xmm3, %xmm2; \
	mulpd	  2 * SIZE(BO, %eax, 4), %xmm3; \
	addpd	%xmm3, %xmm7; \
	movddup	 -6 * SIZE(AO, %eax, 2), %xmm3

#define KERNEL6(address) \
	addpd	%xmm2, %xmm6; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm3, %xmm1; \
	mulpd	  6 * SIZE(BO, %eax, 4), %xmm3; \
	addpd	%xmm1, %xmm4; \
	movapd	  8 * SIZE(BO, %eax, 4), %xmm1; \
	addpd	%xmm3, %xmm5; \
	movddup	 -5 * SIZE(AO, %eax, 2), %xmm3; \
	mulpd	%xmm3, %xmm2; \
	mulpd	  6 * SIZE(BO, %eax, 4), %xmm3; \
	addpd	%xmm3, %xmm7; \
	movddup	 -4 * SIZE(AO, %eax, 2), %xmm3

#define KERNEL7(address) \
	addpd	%xmm2, %xmm6; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm3, %xmm1; \
	mulpd	 10 * SIZE(BO, %eax, 4), %xmm3; \
	addpd	%xmm1, %xmm4; \
	movapd	 12 * SIZE(BO, %eax, 4), %xmm1; \
	addpd	%xmm3, %xmm5; \
	movddup	 -3 * SIZE(AO, %eax, 2), %xmm3; \
	mulpd	%xmm3, %xmm2; \
	mulpd	 10 * SIZE(BO, %eax, 4), %xmm3; \
	addpd	%xmm3, %xmm7; \
	movddup	 -2 * SIZE(AO, %eax, 2), %xmm3

#define KERNEL8(address) \
	addpd	%xmm2, %xmm6; \
	movapd	%xmm1, %xmm2; \
	mulpd	%xmm3, %xmm1; \
	mulpd	 14 * SIZE(BO, %eax, 4), %xmm3; \
	addpd	%xmm1, %xmm4; \
	movapd	 16 * SIZE(BO, %eax, 4), %xmm1; \
	addpd	%xmm3, %xmm5; \
	movddup	 -1 * SIZE(AO, %eax, 2), %xmm3; \
	mulpd	%xmm3, %xmm2; \
	mulpd	 14 * SIZE(BO, %eax, 4), %xmm3; \
	addpd	%xmm3, %xmm7; \
	movddup	  8 * SIZE(AO, %eax, 2), %xmm3; \
	addpd	%xmm2, %xmm6; \
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

	leal	(, LDC, SIZE), LDC

	movl	N,  %eax
	sarl	$2, %eax
	movl	%eax, J
	jle	.L30
	ALIGN_2

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	leal	GEMM_DEFAULT_Q * GEMM_DEFAULT_UNROLL_N * SIZE(B), %eax
	movl	%eax, BX

	movl	C, CO		# coffset = c
	movl	A, AO		# aoffset = a

	movl	M,  I
	sarl	$1, I	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
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

	movddup	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm1
	pxor	%xmm4, %xmm4
	movddup	 -8 * SIZE(AO), %xmm3

	leal	(LDC, LDC, 2), %eax

	prefetchw	1 * SIZE(CO)
	pxor	%xmm5, %xmm5
	prefetchw	3 * SIZE(CO, LDC)
	pxor	%xmm6, %xmm6
	prefetchw	1 * SIZE(CO, LDC, 2)
	pxor	%xmm7, %xmm7
	prefetchw	3 * SIZE(CO, %eax)
	movapd	%xmm1, %xmm2

	movl	BX, %eax
	prefetch   -16 * SIZE(%eax)
	addl	$8 * SIZE, %eax
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
	addl	$2, %eax
#else
	addl	$4, %eax
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
	movddup	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	je .L18

	leal	(, %eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 4), BO
	negl	%eax
	ALIGN_3

.L17:
	mulpd	%xmm0, %xmm1
	mulpd	-14 * SIZE(BO, %eax, 4), %xmm0
	addpd	%xmm1, %xmm4
	movapd	-12 * SIZE(BO, %eax, 4), %xmm1
	addpd	%xmm0, %xmm5
	movddup	-15 * SIZE(AO, %eax, 2), %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	-14 * SIZE(BO, %eax, 4), %xmm0
	addpd	%xmm0, %xmm7
	movddup	-14 * SIZE(AO, %eax, 2), %xmm0
	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2

	addl	$SIZE, %eax
	jl	.L17
	ALIGN_4

.L18:
	leal	(CO, LDC, 2), %eax

	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm5
	mulpd	%xmm3, %xmm6
	mulpd	%xmm3, %xmm7

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO       ), %xmm0
	movhpd	0 * SIZE(CO,   LDC), %xmm0
	movsd	0 * SIZE(%eax     ), %xmm1
	movhpd	0 * SIZE(%eax, LDC), %xmm1

	movsd	1 * SIZE(CO       ), %xmm2
	movhpd	1 * SIZE(CO,   LDC), %xmm2
	movsd	1 * SIZE(%eax     ), %xmm3
	movhpd	1 * SIZE(%eax, LDC), %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm5
	addpd	%xmm2, %xmm6
	addpd	%xmm3, %xmm7
#endif

	movsd	%xmm4, 0 * SIZE(CO)
	movsd	%xmm6, 1 * SIZE(CO)

	movhpd	%xmm4, 0 * SIZE(CO, LDC)
	movhpd	%xmm6, 1 * SIZE(CO, LDC)

	movsd	%xmm5, 0 * SIZE(%eax)
	movsd	%xmm7, 1 * SIZE(%eax)

	movhpd	%xmm5, 0 * SIZE(%eax, LDC)
	movhpd	%xmm7, 1 * SIZE(%eax, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, CO		# coffset += 2
	decl	I			# i --
	jg	.L11
	ALIGN_4

.L20:
	movl	M,  I
	testl	$1, I	# i = (m >> 2)
	jle	.L29

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BO
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AO, %eax, 1), AO
	leal	(B,  %eax, 4), BO
#endif

	movddup	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm1
	movddup	 -8 * SIZE(AO), %xmm3

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
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
	sarl	$3, %eax
	je	.L25
	ALIGN_4

.L22:
	mulpd	%xmm0, %xmm1
	PREFETCH (PREFETCHSIZE  + 0) * SIZE(AO)
	mulpd	-14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm4
	movapd	-12 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm5
	movddup	-15 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm6
	movapd	 -8 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm7
	movddup	-14 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	 -6 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm4
	movapd	 -4 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm5
	movddup	-13 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	 -2 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm6
	movapd	          (BO), %xmm1
	addpd	%xmm0, %xmm7
	movddup	-12 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	  2 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm4
	movapd	  4 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm5
	movddup	-11 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	  6 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm6
	movapd	  8 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm7
	movddup	-10 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	 10 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm4
	movapd	 12 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm5
	movddup	 -9 * SIZE(AO), %xmm0
	mulpd	%xmm0, %xmm1
	mulpd	 14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm6
	movapd	 16 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm7
	movddup	 -8 * SIZE(AO), %xmm0

	subl   $ -8 * SIZE, AO
	subl   $-32 * SIZE, BO
	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
	movddup	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L28

.L26:
	mulpd	%xmm0, %xmm1
	mulpd	-14 * SIZE(BO), %xmm0
	addpd	%xmm1, %xmm4
	movapd	-12 * SIZE(BO), %xmm1
	addpd	%xmm0, %xmm5
	movddup	-15 * SIZE(AO), %xmm0

	addl	$1 * SIZE, AO
	addl	$4 * SIZE, BO
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	leal	(CO, LDC, 2), %eax

	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm5

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO     ), %xmm0
	movhpd	0 * SIZE(CO, LDC), %xmm0
	movsd	0 * SIZE(%eax     ), %xmm1
	movhpd	0 * SIZE(%eax, LDC), %xmm1

	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm5
#endif

	movsd	%xmm4, 0 * SIZE(CO     )
	movhpd	%xmm4, 0 * SIZE(CO, LDC)
	movsd	%xmm5, 0 * SIZE(%eax     )
	movhpd	%xmm5, 0 * SIZE(%eax, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AO, %eax, 1), AO
	leal	(BO, %eax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_4

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	movl	BO, B

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

	movl	C, CO		# coffset = c
	movl	A, AO		# aoffset = a

	movl	M,  I
	sarl	$1, I	# i = (m >> 2)
	jle	.L50
	ALIGN_4

.L41:
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
	prefetchw 1 * SIZE(CO)
	pxor	%xmm5, %xmm5
	prefetchw 1 * SIZE(CO, LDC)
	pxor	%xmm6, %xmm6
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
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AO)
	mulpd	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm4
	mulpd	-16 * SIZE(BO), %xmm1
	movddup	-14 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm5

	mulpd	-14 * SIZE(BO), %xmm0
	movddup	-13 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm6
	mulpd	-14 * SIZE(BO), %xmm1
	movddup	-12 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm7

	mulpd	-12 * SIZE(BO), %xmm0
	movddup	-11 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm4
	mulpd	-12 * SIZE(BO), %xmm1
	movddup	-10 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm5

	mulpd	-10 * SIZE(BO), %xmm0
	movddup	 -9 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm6
	mulpd	-10 * SIZE(BO), %xmm1
	movddup	 -8 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm7

	prefetcht0 (PREFETCHSIZE  + 8) * SIZE(AO)

	mulpd	 -8 * SIZE(BO), %xmm0
	movddup	 -7 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm4
	mulpd	 -8 * SIZE(BO), %xmm1
	movddup	 -6 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm5

	mulpd	 -6 * SIZE(BO), %xmm0
	movddup	 -5 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm6
	mulpd	 -6 * SIZE(BO), %xmm1
	movddup	 -4 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm7

	mulpd	 -4 * SIZE(BO), %xmm0
	movddup	 -3 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm4
	mulpd	 -4 * SIZE(BO), %xmm1
	movddup	 -2 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm5

	mulpd	 -2 * SIZE(BO), %xmm0
	movddup	 -1 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm6
	mulpd	 -2 * SIZE(BO), %xmm1
	movddup	  0 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm7

	subl   $-16 * SIZE, AO
	subl   $-16 * SIZE, BO
	decl   %eax
	jne    .L42
	ALIGN_4

.L45:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movddup	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L48
	ALIGN_3

.L46:
	mulpd	-16 * SIZE(BO), %xmm0
	movddup	-15 * SIZE(AO), %xmm1
	addpd	%xmm0, %xmm4
	mulpd	-16 * SIZE(BO), %xmm1
	movddup	-14 * SIZE(AO), %xmm0
	addpd	%xmm1, %xmm5

	addl	$2 * SIZE, AO
	addl	$2 * SIZE, BO
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO), %xmm0
	movhpd	0 * SIZE(CO, LDC), %xmm0

	movsd	1 * SIZE(CO), %xmm1
	movhpd	1 * SIZE(CO, LDC), %xmm1
#endif

	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm5

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm5
#endif

	movlpd	%xmm4, 0 * SIZE(CO)
	movlpd	%xmm5, 1 * SIZE(CO)

	movhpd	%xmm4, 0 * SIZE(CO, LDC)
	movhpd	%xmm5, 1 * SIZE(CO, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, CO		# coffset += 2
	decl	I			# i --
	jg	.L41
	ALIGN_4

.L50:
	movl	M,  I
	testl	$1, I	# i = (m >> 2)
	jle	.L59

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BO
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AO, %eax, 1), AO
	leal	(B,  %eax, 2), BO
#endif

	movddup -16 * SIZE(AO), %xmm0
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
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
	sarl	$3, %eax
	je	.L55
	ALIGN_4

.L52:
	mulpd	-16 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-15 * SIZE(AO), %xmm0

	mulpd	-14 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-14 * SIZE(AO), %xmm0

	mulpd	-12 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-13 * SIZE(AO), %xmm0

	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-12 * SIZE(AO), %xmm0

	mulpd	 -8 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-11 * SIZE(AO), %xmm0

	mulpd	 -6 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-10 * SIZE(AO), %xmm0

	mulpd	 -4 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	 -9 * SIZE(AO), %xmm0

	mulpd	 -2 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	 -8 * SIZE(AO), %xmm0

	subl   $ -8 * SIZE, AO
	subl   $-16 * SIZE, BO

	decl   %eax
	jne    .L52
	ALIGN_4

.L55:
	movddup	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L58

.L56:
	mulpd	-16 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-15 * SIZE(AO), %xmm0

	subl	$-1 * SIZE, AO
	subl	$-2 * SIZE, BO
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm4

	mulpd	%xmm3, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO), %xmm0
	movhpd	0 * SIZE(CO, LDC), %xmm0

	addpd	%xmm0, %xmm4
#endif

	movlpd	%xmm4, 0 * SIZE(CO)
	movhpd	%xmm4, 0 * SIZE(CO, LDC, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AO, %eax, 1), AO
	leal	(BO, %eax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_4

.L59:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	movl	BO, B

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

	movl	C, CO		# coffset = c
	movl	A, AO		# aoffset = a

	movl	M,  I
	sarl	$1, I	# i = (m >> 2)
	jle	.L80
	ALIGN_4

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BO
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(B,  %eax, 1), BO
#endif

	movddup	-16 * SIZE(BO), %xmm0
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	prefetchw   1 * SIZE(CO)

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
	PREFETCH (PREFETCHSIZE  + 0) * SIZE(AO)

	mulpd	-16 * SIZE(AO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-15 * SIZE(BO), %xmm0

	mulpd	-14 * SIZE(AO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-14 * SIZE(BO), %xmm0

	mulpd	-12 * SIZE(AO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-13 * SIZE(BO), %xmm0

	mulpd	-10 * SIZE(AO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-12 * SIZE(BO), %xmm0

	PREFETCH (PREFETCHSIZE  + 8) * SIZE(AO)

	mulpd	 -8 * SIZE(AO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-11 * SIZE(BO), %xmm0

	mulpd	 -6 * SIZE(AO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-10 * SIZE(BO), %xmm0

	mulpd	 -4 * SIZE(AO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	 -9 * SIZE(BO), %xmm0

	mulpd	 -2 * SIZE(AO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	 -8 * SIZE(BO), %xmm0

	subl   $-16 * SIZE, AO
	subl   $ -8 * SIZE, BO
	decl   %eax
	jne    .L72
	ALIGN_4

.L75:
	movddup	ALPHA,  %xmm3
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_3

.L76:
	mulpd	-16 * SIZE(AO), %xmm0
	addpd	%xmm0, %xmm4
	movddup	-15 * SIZE(BO), %xmm0

	addl	$2 * SIZE, AO
	addl	$1 * SIZE, BO
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
	mulpd	%xmm3, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO), %xmm0
	movhpd	1 * SIZE(CO), %xmm0

	addpd	%xmm0, %xmm4
#endif

	movsd	%xmm4, 0 * SIZE(CO)
	movhpd	%xmm4, 1 * SIZE(CO)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, CO		# coffset += 2
	decl	I			# i --
	jg	.L71
	ALIGN_4

.L80:
	movl	M,  I
	testl	$1, I	# i = (m >> 2)
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BO
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AO, %eax, 1), AO
	leal	(B,  %eax, 1), BO
#endif

	movaps	-16 * SIZE(AO), %xmm0
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
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
	je	.L85
	ALIGN_4

.L82:
	PREFETCH (PREFETCHSIZE  + 0) * SIZE(AO)

	mulpd	-16 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm4
	movapd	-14 * SIZE(AO), %xmm0

	mulpd	-14 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm5
	movapd	-12 * SIZE(AO), %xmm0

	mulpd	-12 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm6
	movapd	-10 * SIZE(AO), %xmm0

	mulpd	-10 * SIZE(BO), %xmm0
	addpd	%xmm0, %xmm7
	movapd	 -8 * SIZE(AO), %xmm0

	subl   $-8 * SIZE, AO
	subl   $-8 * SIZE, BO
	decl   %eax
	jne    .L82
	ALIGN_4

.L85:
	movddup	ALPHA,  %xmm3
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L88

.L86:
	mulsd	-16 * SIZE(BO), %xmm0
	addsd	%xmm0, %xmm4
	movsd	-15 * SIZE(AO), %xmm0

	addl	$1 * SIZE, AO
	addl	$1 * SIZE, BO
	decl	%eax
	jg	.L86
	ALIGN_4

.L88:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
	addpd	%xmm6, %xmm4

	haddpd	%xmm4, %xmm4
	mulsd	%xmm3, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO), %xmm0
	addsd	%xmm0, %xmm4
#endif
	movsd	%xmm4, 0 * SIZE(CO)
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
