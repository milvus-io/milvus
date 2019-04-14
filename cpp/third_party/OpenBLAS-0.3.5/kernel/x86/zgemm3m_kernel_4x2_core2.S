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
#define BUFFER 256(%esp)

#define PREFETCH_R    (8 * 16 + 0)
#define PREFETCH_W    (PREFETCH_R * 2)

#define PREFETCHSIZE  (8 * 7 + 4)
#define PREFETCH     prefetcht0

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define C1	%esi
#define I	%ebx

	PROLOGUE
	PROFCODE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	movl	%esp, %esi	# save old stack

	subl	$512 + LOCAL_BUFFER_SIZE, %esp
	andl	$-4096, %esp	# align stack

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

	movl	OLD_B, B
	movl	OLD_C, %ebx

	movaps	 %xmm0, ALPHA
	movl	%ebx, C
	movl	OLD_LDC, LDC

	subl	$-16 * SIZE, A
	subl	$-16 * SIZE, B

	sall	$ZBASE_SHIFT, LDC

	sarl	$1, %eax
	movl	%eax, J
	jle	.L40
	ALIGN_4

.L01:
	leal	16 * SIZE + BUFFER, BB

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sarl	$2, %eax
	jle	.L05
	ALIGN_4

.L02:
	movddup	 -16 * SIZE(B), %xmm0
	movddup	 -15 * SIZE(B), %xmm1
	movddup	 -14 * SIZE(B), %xmm2
	movddup	 -13 * SIZE(B), %xmm3
	movddup	 -12 * SIZE(B), %xmm4
	movddup	 -11 * SIZE(B), %xmm5
	movddup	 -10 * SIZE(B), %xmm6
	movddup	  -9 * SIZE(B), %xmm7

	prefetcht0	(PREFETCH_R + 0) * SIZE(B)

	movapd	%xmm0,  -16 * SIZE(BB)
	movapd	%xmm1,  -14 * SIZE(BB)
	movapd	%xmm2,  -12 * SIZE(BB)
	movapd	%xmm3,  -10 * SIZE(BB)

	movapd	%xmm4,   -8 * SIZE(BB)
	movapd	%xmm5,   -6 * SIZE(BB)
	movapd	%xmm6,   -4 * SIZE(BB)
	movapd	%xmm7,   -2 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, BB
	decl	%eax
	jne	.L02
	ALIGN_4

.L05:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L10
	ALIGN_4

.L06:
	movddup	 -16 * SIZE(B), %xmm0
	movddup	 -15 * SIZE(B), %xmm1

	movapd	%xmm0,  -16 * SIZE(BB)
	movapd	%xmm1,  -14 * SIZE(BB)
	addl	$2 * SIZE, B
	addl	$4 * SIZE, BB
	decl	%eax
	jne	.L06
	ALIGN_4

.L10:
	movl	B, BX

	movl	C, C1
	movl	A, AA
	movl	M,  I
	sarl	$2, I
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	16 * SIZE + BUFFER, BB
#else
	leal	16 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */
#endif

	movapd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 -8 * SIZE(AA), %xmm3
	pxor	%xmm6, %xmm6
	prefetcht0	3 * SIZE(C1)
	pxor	%xmm7, %xmm7
	prefetcht0	3 * SIZE(C1, LDC)
	movapd	%xmm1,  %xmm2

	movl	BX, %eax
	prefetcht0   (%eax)
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
	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	mulpd	%xmm0,  %xmm1
	addpd	%xmm1,  %xmm4
	movapd	-14 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm0
	addpd	%xmm0,  %xmm5
	movapd	-14 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm2
	addpd	%xmm2,  %xmm6
	movapd	-12 * SIZE(BB), %xmm2
	mulpd	%xmm0,  %xmm1
	movapd	-12 * SIZE(AA), %xmm0
	addpd	%xmm1,  %xmm7
	PADDING;
	movapd	%xmm2,  %xmm1

	mulpd	%xmm0,  %xmm2
	addpd	%xmm2,  %xmm4
	movapd	-10 * SIZE(BB), %xmm2
	mulpd	%xmm2,  %xmm0
	addpd	%xmm0,  %xmm5
	movapd	-10 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm1
	addpd	%xmm1,  %xmm6
	movapd	 -8 * SIZE(BB), %xmm1
	mulpd	%xmm0,  %xmm2
	PADDING;
	movapd	  0 * SIZE(AA), %xmm0
	addpd	%xmm2,  %xmm7
	PADDING;
	movapd	%xmm1,  %xmm2

	mulpd	%xmm3,  %xmm1
	addpd	%xmm1,  %xmm4
	movapd	 -6 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm3
	addpd	%xmm3,  %xmm5
	movapd	 -6 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm2
	addpd	%xmm2,  %xmm6
	movapd	 -4 * SIZE(BB), %xmm2
	mulpd	%xmm3,  %xmm1
	movapd	 -4 * SIZE(AA), %xmm3
	addpd	%xmm1,  %xmm7
	PADDING;
	movapd	%xmm2,  %xmm1

	mulpd	%xmm3,  %xmm2
	addpd	%xmm2,  %xmm4
	movapd	 -2 * SIZE(BB), %xmm2
	mulpd	%xmm2,  %xmm3
	addpd	%xmm3,  %xmm5
	movapd	 -2 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm1
	addpd	%xmm1,  %xmm6
	PADDING;
	movapd	  0 * SIZE(BB), %xmm1
	mulpd	%xmm3,  %xmm2
	movapd	  8 * SIZE(AA), %xmm3
	addpd	%xmm2,  %xmm7
	PADDING;
	movapd	%xmm1,  %xmm2

	mulpd	%xmm0,  %xmm1
	addpd	%xmm1,  %xmm4
	movapd	  2 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm0
	addpd	%xmm0,  %xmm5
	movapd	  2 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm2
	addpd	%xmm2,  %xmm6
	movapd	  4 * SIZE(BB), %xmm2
	mulpd	%xmm0,  %xmm1
	movapd	  4 * SIZE(AA), %xmm0
	addpd	%xmm1,  %xmm7
	PADDING;
	movapd	%xmm2,  %xmm1

	mulpd	%xmm0,  %xmm2
	addpd	%xmm2,  %xmm4
	movapd	  6 * SIZE(BB), %xmm2
	mulpd	%xmm2,  %xmm0
	addpd	%xmm0,  %xmm5
	movapd	  6 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm1
	addpd	%xmm1,  %xmm6
	movapd	  8 * SIZE(BB), %xmm1
	mulpd	%xmm0,  %xmm2
	movapd	 16 * SIZE(AA), %xmm0
	addpd	%xmm2,  %xmm7
	PADDING;
	movapd	%xmm1,  %xmm2

	mulpd	%xmm3,  %xmm1
	addpd	%xmm1,  %xmm4
	movapd	 10 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm3
	addpd	%xmm3,  %xmm5
	movapd	 10 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm2
	addpd	%xmm2,  %xmm6
	movapd	 12 * SIZE(BB), %xmm2
	mulpd	%xmm3,  %xmm1
	movapd	 12 * SIZE(AA), %xmm3
	addpd	%xmm1,  %xmm7
	PADDING;
	movapd	%xmm2,  %xmm1

	mulpd	%xmm3,  %xmm2
	addpd	%xmm2,  %xmm4
	movapd	 14 * SIZE(BB), %xmm2
	mulpd	%xmm2,  %xmm3
	subl   $-32 * SIZE, BB
	addpd	%xmm3,  %xmm5
	movapd	 14 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm1
	addpd	%xmm1,  %xmm6
	movapd	-16 * SIZE(BB), %xmm1
	mulpd	%xmm3,  %xmm2
	movapd	 24 * SIZE(AA), %xmm3
	addpd	%xmm2,  %xmm7
	PADDING;
	movapd	%xmm1,  %xmm2

	subl   $-32 * SIZE, AA
	decl   %eax
	BRANCH
	jne  .L12
	ALIGN_4

.L15:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3

	andl	$7, %eax
	BRANCH
	je .L18
	ALIGN_4

.L16:
	mulpd	%xmm0,  %xmm1
	addpd	%xmm1,  %xmm4
	movapd	-14 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm0
	addpd	%xmm0,  %xmm5
	movapd	-14 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm2
	mulpd	%xmm0,  %xmm1
	movapd	-12 * SIZE(AA), %xmm0
	addpd	%xmm2,  %xmm6
	addpd	%xmm1,  %xmm7
	movapd	-12 * SIZE(BB), %xmm1
	movapd	%xmm1,  %xmm2

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
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

	movsd	4 * SIZE(%esi), %xmm0
	movhps	5 * SIZE(%esi), %xmm0
	movsd	6 * SIZE(%esi), %xmm1
	movhps	7 * SIZE(%esi), %xmm1

	pshufd  $0x44,  %xmm6,  %xmm2
	unpckhpd %xmm6, %xmm6

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm6
	addpd	 %xmm6,  %xmm1

	movlps	%xmm0, 4 * SIZE(%esi)
	movhps	%xmm0, 5 * SIZE(%esi)
	movlps	%xmm1, 6 * SIZE(%esi)
	movhps	%xmm1, 7 * SIZE(%esi)

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

	movsd	4 * SIZE(%esi, LDC), %xmm0
	movhps	5 * SIZE(%esi, LDC), %xmm0
	movsd	6 * SIZE(%esi, LDC), %xmm1
	movhps	7 * SIZE(%esi, LDC), %xmm1

	pshufd  $0x44,  %xmm7,  %xmm2
	unpckhpd %xmm7, %xmm7

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm7
	addpd	 %xmm7,  %xmm1

	movlps	%xmm0, 4 * SIZE(%esi, LDC)
	movhps	%xmm0, 5 * SIZE(%esi, LDC)
	movlps	%xmm1, 6 * SIZE(%esi, LDC)
	movhps	%xmm1, 7 * SIZE(%esi, LDC)

	addl	$8 * SIZE, C1
	decl	I
	jg	.L11
	ALIGN_4

.L20:
	movl	M,  I
	testl	$2, I
	jle	.L30

.L21:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	16 * SIZE + BUFFER, BB
#else
	leal	16 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */
#endif

	movapd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 -8 * SIZE(AA), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 -8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

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
	je	.L25
	ALIGN_4

.L22:
	mulpd	%xmm0,  %xmm1
	mulpd	-14 * SIZE(BB), %xmm0
	addpd	%xmm1, %xmm4
	movapd	-12 * SIZE(BB), %xmm1
	addpd	%xmm0, %xmm5
	movapd	-14 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm1
	mulpd	-10 * SIZE(BB), %xmm0
	addpd	%xmm1, %xmm6
	movapd	  0 * SIZE(BB), %xmm1
	addpd	%xmm0, %xmm7
	movapd	-12 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm3
	mulpd	 -6 * SIZE(BB), %xmm0
	addpd	%xmm3, %xmm4
	movapd	 -4 * SIZE(BB), %xmm3
	addpd	%xmm0, %xmm5
	movapd	-10 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm3
	mulpd	 -2 * SIZE(BB), %xmm0
	addpd	%xmm3, %xmm6
	movapd	  8 * SIZE(BB), %xmm3
	addpd	%xmm0, %xmm7
	movapd	  0 * SIZE(AA), %xmm0
	mulpd	%xmm2,  %xmm1
	mulpd	  2 * SIZE(BB), %xmm2
	addpd	%xmm1, %xmm4
	movapd	  4 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm5
	movapd	 -6 * SIZE(AA), %xmm2
	mulpd	%xmm2,  %xmm1
	mulpd	  6 * SIZE(BB), %xmm2
	addpd	%xmm1, %xmm6
	movapd	 16 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm7
	movapd	 -4 * SIZE(AA), %xmm2
	mulpd	%xmm2,  %xmm3
	mulpd	 10 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm4
	movapd	 12 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm5
	movapd	 -2 * SIZE(AA), %xmm2
	mulpd	%xmm2,  %xmm3
	mulpd	 14 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm6
	movapd	 24 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm7
	movapd	  8 * SIZE(AA), %xmm2

	subl   $-16 * SIZE, AA
	addl   $ 32 * SIZE, BB
	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L28
	ALIGN_4

.L26:
	mulpd	%xmm0,  %xmm1
	mulpd	-14 * SIZE(BB), %xmm0
	addpd	%xmm1, %xmm4
	movapd	-12 * SIZE(BB), %xmm1
	addpd	%xmm0, %xmm5
	movapd	-14 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
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

	addl	$4 * SIZE, C1
	ALIGN_4

.L30:
	movl	M,  I
	testl	$1, I
	jle	.L39

.L31:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	16 * SIZE + BUFFER, BB
#else
	leal	16 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */
#endif

	movsd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movsd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movsd	-12 * SIZE(AA), %xmm2
	pxor	%xmm6, %xmm6
	movsd	 -8 * SIZE(BB), %xmm3
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
	je	.L35
	ALIGN_4

.L32:
	mulsd	%xmm0,  %xmm1
	mulsd	-14 * SIZE(BB), %xmm0
	addsd	%xmm1, %xmm4
 	movsd	-12 * SIZE(BB), %xmm1
	addsd	%xmm0, %xmm5
	movsd	-15 * SIZE(AA), %xmm0
	mulsd	%xmm0,  %xmm1
	mulsd	-10 * SIZE(BB), %xmm0
	addsd	%xmm1, %xmm6
	movsd	  0 * SIZE(BB), %xmm1
	addsd	%xmm0, %xmm7
	movsd	-14 * SIZE(AA), %xmm0
	mulsd	%xmm0,  %xmm3
	mulsd	 -6 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm4
	movsd	 -4 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm5
	movsd	-13 * SIZE(AA), %xmm0
	mulsd	%xmm0,  %xmm3
	mulsd	 -2 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm6
	movsd	  8 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm7
	movsd	 -8 * SIZE(AA), %xmm0
	mulsd	%xmm2,  %xmm1
	mulsd	  2 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm4
	movsd	  4 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm5
	movsd	-11 * SIZE(AA), %xmm2
	mulsd	%xmm2,  %xmm1
	mulsd	  6 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm6
	movsd	 16 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm7
	movsd	-10 * SIZE(AA), %xmm2
	mulsd	%xmm2,  %xmm3
	mulsd	 10 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm4
	movsd	 12 * SIZE(BB), %xmm3
	addsd	%xmm2, %xmm5
	movsd	 -9 * SIZE(AA), %xmm2
	mulsd	%xmm2,  %xmm3
	mulsd	 14 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm6
	movsd	 24 * SIZE(BB), %xmm3
	addsd	%xmm2, %xmm7
	movsd	 -4 * SIZE(AA), %xmm2

	subl   $-8 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L32
	ALIGN_4

.L35:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L38
	ALIGN_4

.L36:
	mulsd	%xmm0,  %xmm1
	mulsd	-14 * SIZE(BB), %xmm0
	addsd	%xmm1, %xmm4
	movsd	-12 * SIZE(BB), %xmm1
	addsd	%xmm0, %xmm5
	movsd	-15 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
	addsd	%xmm6, %xmm4
	addsd	%xmm7, %xmm5

	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0
	movsd	0 * SIZE(%esi, LDC), %xmm1
	movhps	1 * SIZE(%esi, LDC), %xmm1

	unpcklpd  %xmm4, %xmm4
	unpcklpd  %xmm5, %xmm5

	mulpd	 %xmm3,  %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 %xmm3,  %xmm5
	addpd	 %xmm5,  %xmm1

	movlps	%xmm0, 0 * SIZE(%esi)
	movhps	%xmm0, 1 * SIZE(%esi)
	movlps	%xmm1, 0 * SIZE(%esi, LDC)
	movhps	%xmm1, 1 * SIZE(%esi, LDC)
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leal	(, LDC, 2), %eax
	addl	%eax, C
	decl	J
	jg	.L01
	ALIGN_4

.L40:
	movl	N, %eax
	testl	$1, %eax
	jle	.L999
	ALIGN_4

.L41:
	leal	16 * SIZE + BUFFER, BB

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sarl	$3, %eax
	jle	.L45
	ALIGN_4

.L42:
	movddup	 -16 * SIZE(B), %xmm0
	movddup	 -15 * SIZE(B), %xmm1
	movddup	 -14 * SIZE(B), %xmm2
	movddup	 -13 * SIZE(B), %xmm3
	movddup	 -12 * SIZE(B), %xmm4
	movddup	 -11 * SIZE(B), %xmm5
	movddup	 -10 * SIZE(B), %xmm6
	movddup	  -9 * SIZE(B), %xmm7

	movapd	%xmm0,  -16 * SIZE(BB)
	movapd	%xmm1,  -14 * SIZE(BB)
	movapd	%xmm2,  -12 * SIZE(BB)
	movapd	%xmm3,  -10 * SIZE(BB)
	movapd	%xmm4,   -8 * SIZE(BB)
	movapd	%xmm5,   -6 * SIZE(BB)
	movapd	%xmm6,   -4 * SIZE(BB)
	movapd	%xmm7,   -2 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, BB
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
	movddup	 -16 * SIZE(B), %xmm0

	movapd	%xmm0,  -16 * SIZE(BB)
	addl	$1 * SIZE, B
	addl	$2 * SIZE, BB
	decl	%eax
	jne	.L46
	ALIGN_4

.L50:
	movl	C, C1
	movl	A, AA
	movl	M,  I
	sarl	$2, I
	jle	.L60
	ALIGN_4

.L51:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	16 * SIZE + BUFFER, BB
#else
	leal	16 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB
#endif

	movapd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 -8 * SIZE(AA), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 -8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

	prefetcht0	3 * SIZE(C1)

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
	je	.L55
	ALIGN_4

.L52:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AA), %xmm1
	addpd	%xmm0, %xmm4
	movapd	-12 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm6
	movapd	-14 * SIZE(BB), %xmm1
	mulpd	%xmm1, %xmm0
	mulpd	-10 * SIZE(AA), %xmm1
	addpd	%xmm0, %xmm5
	movapd	  0 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm7
	movapd	-12 * SIZE(BB), %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	 -6 * SIZE(AA), %xmm1
	addpd	%xmm2, %xmm4
	movapd	 -4 * SIZE(AA), %xmm2
	addpd	%xmm1, %xmm6
	movapd	-10 * SIZE(BB), %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	 -2 * SIZE(AA), %xmm1
	addpd	%xmm2, %xmm5
	movapd	  8 * SIZE(AA), %xmm2
	addpd	%xmm1, %xmm7
	movapd	  0 * SIZE(BB), %xmm1
	mulpd	%xmm3, %xmm0
	mulpd	  2 * SIZE(AA), %xmm3
	addpd	%xmm0, %xmm4
	movapd	  4 * SIZE(AA), %xmm0
	addpd	%xmm3, %xmm6
	movapd	 -6 * SIZE(BB), %xmm3
	mulpd	%xmm3, %xmm0
	mulpd	  6 * SIZE(AA), %xmm3
	addpd	%xmm0, %xmm5
	movapd	 16 * SIZE(AA), %xmm0
	addpd	%xmm3, %xmm7
	movapd	 -4 * SIZE(BB), %xmm3
	mulpd	%xmm3, %xmm2
	mulpd	 10 * SIZE(AA), %xmm3
	addpd	%xmm2, %xmm4
	movapd	 12 * SIZE(AA), %xmm2
	addpd	%xmm3, %xmm6
	movapd	 -2 * SIZE(BB), %xmm3
	mulpd	%xmm3, %xmm2
	mulpd	 14 * SIZE(AA), %xmm3
	addpd	%xmm2, %xmm5
	movapd	 24 * SIZE(AA), %xmm2
	addpd	%xmm3, %xmm7
	movapd	  8 * SIZE(BB), %xmm3

	addl   $ 32 * SIZE, AA
	subl   $-16 * SIZE, BB
	decl   %eax
	jne    .L52
	ALIGN_4

.L55:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L58
	ALIGN_4

.L56:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AA), %xmm1
	addpd	%xmm0, %xmm4
	movapd	-12 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm6
	movapd	-14 * SIZE(BB), %xmm1

	addl	$4 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6

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

	movsd	4 * SIZE(%esi), %xmm0
	movhps	5 * SIZE(%esi), %xmm0
	movsd	6 * SIZE(%esi), %xmm1
	movhps	7 * SIZE(%esi), %xmm1

	pshufd  $0x44,  %xmm6,  %xmm2
	unpckhpd %xmm6, %xmm6

	mulpd	 %xmm3,  %xmm2
	addpd	 %xmm2,  %xmm0
	mulpd	 %xmm3,  %xmm6
	addpd	 %xmm6,  %xmm1

	movlps	%xmm0, 4 * SIZE(%esi)
	movhps	%xmm0, 5 * SIZE(%esi)
	movlps	%xmm1, 6 * SIZE(%esi)
	movhps	%xmm1, 7 * SIZE(%esi)

	addl	$8 * SIZE, C1
	decl	I
	jg	.L51
	ALIGN_4

.L60:
	movl	M,  I
	testl	$2, I
	jle	.L70

.L61:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	16 * SIZE + BUFFER, BB
#else
	leal	16 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

	movapd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 -8 * SIZE(AA), %xmm2
	movapd	 -8 * SIZE(BB), %xmm3

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
	je	.L65
	ALIGN_4

.L62:
	mulpd	%xmm0,  %xmm1
	movapd	-14 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm4
	movapd	-14 * SIZE(BB), %xmm1
	mulpd	%xmm0,  %xmm1
	movapd	-12 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movapd	-12 * SIZE(BB), %xmm1
	mulpd	%xmm0,  %xmm1
	movapd	-10 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm4
	movapd	-10 * SIZE(BB), %xmm1
	mulpd	%xmm0,  %xmm1
	movapd	  0 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movapd	  0 * SIZE(BB), %xmm1
	mulpd	%xmm2,  %xmm3
	movapd	 -6 * SIZE(AA), %xmm2
	addpd	%xmm3, %xmm4
	movapd	 -6 * SIZE(BB), %xmm3
	mulpd	%xmm2,  %xmm3
	movapd	 -4 * SIZE(AA), %xmm2
	addpd	%xmm3, %xmm5
	movapd	 -4 * SIZE(BB), %xmm3
	mulpd	%xmm2,  %xmm3
	movapd	 -2 * SIZE(AA), %xmm2
	addpd	%xmm3, %xmm4
	movapd	 -2 * SIZE(BB), %xmm3
	mulpd	%xmm2,  %xmm3
	movapd	  8 * SIZE(AA), %xmm2
	addpd	%xmm3, %xmm5
	movapd	  8 * SIZE(BB), %xmm3

	subl   $-16 * SIZE, AA
	subl   $-16 * SIZE, BB
	decl   %eax
	jne    .L62
	ALIGN_4

.L65:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L68
	ALIGN_4

.L66:
	mulpd	%xmm0,  %xmm1
	movapd	-14 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm4
	movapd	-14 * SIZE(BB), %xmm1

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L66
	ALIGN_4

.L68:
	addpd	%xmm5, %xmm4

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

	addl	$4 * SIZE, C1
	ALIGN_4

.L70:
	movl	M,  I
	testl	$1, I
	jle	.L79

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	16 * SIZE + BUFFER, BB
#else
	leal	16 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

	movsd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movsd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movsd	 -8 * SIZE(BB), %xmm3
	movsd	-12 * SIZE(AA), %xmm2

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
	je	.L75
	ALIGN_4

.L72:
	mulsd	%xmm0,  %xmm1
	movsd	-15 * SIZE(AA), %xmm0
	addsd	%xmm1, %xmm4
	movsd	-14 * SIZE(BB), %xmm1
	mulsd	%xmm0,  %xmm1
	movsd	-14 * SIZE(AA), %xmm0
	addsd	%xmm1, %xmm5
	movsd	-12 * SIZE(BB), %xmm1
	mulsd	%xmm0,  %xmm1
	movsd	-13 * SIZE(AA), %xmm0
	addsd	%xmm1, %xmm4
	movsd	-10 * SIZE(BB), %xmm1
	mulsd	%xmm0,  %xmm1
	movsd	 -8 * SIZE(AA), %xmm0
	addsd	%xmm1, %xmm5
	movsd	 -0 * SIZE(BB), %xmm1
	mulsd	%xmm2,  %xmm3
	movsd	-11 * SIZE(AA), %xmm2
	addsd	%xmm3, %xmm4
	movsd	 -6 * SIZE(BB), %xmm3
	mulsd	%xmm2,  %xmm3
	movsd	-10 * SIZE(AA), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 -4 * SIZE(BB), %xmm3
	mulsd	%xmm2,  %xmm3
	movsd	 -9 * SIZE(AA), %xmm2
	addsd	%xmm3, %xmm4
	movsd	 -2 * SIZE(BB), %xmm3
	mulsd	%xmm2,  %xmm3
	movsd	 -4 * SIZE(AA), %xmm2
	addsd	%xmm3, %xmm5
	movsd	  8 * SIZE(BB), %xmm3

	subl   $ -8 * SIZE, AA
	subl   $-16 * SIZE, BB
	decl   %eax
	jne    .L72
	ALIGN_4

.L75:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L78
	ALIGN_4

.L76:
	mulsd	%xmm0,  %xmm1
	movsd	-15 * SIZE(AA), %xmm0
	addsd	%xmm1, %xmm4
	movsd	-14 * SIZE(BB), %xmm1

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
	addsd	%xmm5, %xmm4

	movsd	0 * SIZE(%esi), %xmm0
	movhps	1 * SIZE(%esi), %xmm0

	unpcklpd %xmm4, %xmm4

	mulpd	 %xmm3,  %xmm4
	addpd	 %xmm4,  %xmm0

	movlps	%xmm0, 0 * SIZE(%esi)
	movhps	%xmm0, 1 * SIZE(%esi)
	ALIGN_4

.L79:
	addl	LDC, C
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
