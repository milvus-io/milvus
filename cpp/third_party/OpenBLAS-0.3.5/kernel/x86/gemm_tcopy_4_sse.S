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

#define RPREFETCHSIZE	8
#define WPREFETCHSIZE (RPREFETCHSIZE * 4)
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht2

#define STACK	16
#define ARGS	 0

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define ARG_A	12 + STACK + ARGS(%esp)
#define ARG_LDA	16 + STACK + ARGS(%esp)
#define ARG_B	20 + STACK + ARGS(%esp)

#define A	%eax
#define B	%ebx
#define LDA	%ebp
#define A1	%ecx
#define A2	%edx
#define I	%esi
#define J	%edi

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	ARG_A, A
	movl	ARG_B, B
	movl	ARG_LDA, LDA

	sall	$BASE_SHIFT, LDA

	movl	N,  J
	sarl	$2, J
	je	.L20
	ALIGN_3

.L10:
	movl	A, A1
	leal	(A, LDA, 2), A2
	addl	$4 * SIZE, A

	movl	M,  I
	sarl	$2, I
	je	.L15
	ALIGN_3

.L12:
	PREFETCH	RPREFETCHSIZE * SIZE(A1)

	movsd	0 * SIZE(A1)     , %xmm0
	movhps	1 * SIZE(A1)     , %xmm0
	movsd	2 * SIZE(A1)     , %xmm1
	movhps	3 * SIZE(A1)     , %xmm1

	PREFETCH	RPREFETCHSIZE * SIZE(A1, LDA)

	movsd	0 * SIZE(A1, LDA), %xmm2
	movhps	1 * SIZE(A1, LDA), %xmm2
	movsd	2 * SIZE(A1, LDA), %xmm3
	movhps	3 * SIZE(A1, LDA), %xmm3

	PREFETCH	RPREFETCHSIZE * SIZE(A2)

	movsd	0 * SIZE(A2)     , %xmm4
	movhps	1 * SIZE(A2)     , %xmm4
	movsd	2 * SIZE(A2)     , %xmm5
	movhps	3 * SIZE(A2)     , %xmm5

	PREFETCH	RPREFETCHSIZE * SIZE(A2, LDA)

	movsd	0 * SIZE(A2, LDA), %xmm6
	movhps	1 * SIZE(A2, LDA), %xmm6
	movsd	2 * SIZE(A2, LDA), %xmm7
	movhps	3 * SIZE(A2, LDA), %xmm7

	PREFETCHW	(RPREFETCHSIZE + 0) * SIZE(B)

	movaps	%xmm0, 0 * SIZE(B)
	movaps	%xmm1, 2 * SIZE(B)
	movaps	%xmm2, 4 * SIZE(B)
	movaps	%xmm3, 6 * SIZE(B)

	PREFETCHW	(RPREFETCHSIZE + 8) * SIZE(B)

	movaps	%xmm4,  8 * SIZE(B)
	movaps	%xmm5, 10 * SIZE(B)
	movaps	%xmm6, 12 * SIZE(B)
	movaps	%xmm7, 14 * SIZE(B)

	leal	(A1, LDA, 4), A1
	leal	(A2, LDA, 4), A2
	subl	$-16 * SIZE, B
	decl	I
	jne	.L12
	ALIGN_3

.L15:
	testl	$2, M
	jle	.L16

	movsd	0 * SIZE(A1)     , %xmm0
	movhps	1 * SIZE(A1)     , %xmm0
	movsd	2 * SIZE(A1)     , %xmm1
	movhps	3 * SIZE(A1)     , %xmm1

	movsd	0 * SIZE(A1, LDA), %xmm2
	movhps	1 * SIZE(A1, LDA), %xmm2
	movsd	2 * SIZE(A1, LDA), %xmm3
	movhps	3 * SIZE(A1, LDA), %xmm3

	movaps	%xmm0, 0 * SIZE(B)
	movaps	%xmm1, 2 * SIZE(B)
	movaps	%xmm2, 4 * SIZE(B)
	movaps	%xmm3, 6 * SIZE(B)

	leal	(A1, LDA, 2), A1
	subl	$-8 * SIZE, B
	ALIGN_4

.L16:
	testl	$1, M
	jle	.L19

	movsd	0 * SIZE(A1)     , %xmm0
	movhps	1 * SIZE(A1)     , %xmm0
	movsd	2 * SIZE(A1)     , %xmm1
	movhps	3 * SIZE(A1)     , %xmm1

	movaps	%xmm0, 0 * SIZE(B)
	movaps	%xmm1, 2 * SIZE(B)
	subl	$-4 * SIZE, B
	ALIGN_4

.L19:
	decl	J
	jne	.L10
	ALIGN_3

.L20:
	testl	$2, N
	jle	.L30

	movl	A, A1
	leal	(A, LDA, 2), A2
	addl	$2 * SIZE, A

	movl	M,  I
	sarl	$2, I
	je	.L25
	ALIGN_3

.L22:
	movsd	0 * SIZE(A1)     , %xmm0
	movhps	1 * SIZE(A1)     , %xmm0
	movsd	0 * SIZE(A1, LDA), %xmm1
	movhps	1 * SIZE(A1, LDA), %xmm1

	movsd	0 * SIZE(A2)     , %xmm2
	movhps	1 * SIZE(A2)     , %xmm2
	movsd	0 * SIZE(A2, LDA), %xmm3
	movhps	1 * SIZE(A2, LDA), %xmm3

	movaps	%xmm0, 0 * SIZE(B)
	movaps	%xmm1, 2 * SIZE(B)
	movaps	%xmm2, 4 * SIZE(B)
	movaps	%xmm3, 6 * SIZE(B)

	leal	(A1, LDA, 4), A1
	leal	(A2, LDA, 4), A2
	subl	$-8 * SIZE, B
	decl	I
	jne	.L22
	ALIGN_3

.L25:
	testl	$2, M
	jle	.L26

	movsd	0 * SIZE(A1)     , %xmm0
	movhps	1 * SIZE(A1)     , %xmm0
	movsd	0 * SIZE(A1, LDA), %xmm1
	movhps	1 * SIZE(A1, LDA), %xmm1

	movaps	%xmm0, 0 * SIZE(B)
	movaps	%xmm1, 2 * SIZE(B)

	leal	(A1, LDA, 2), A1
	subl	$-4 * SIZE, B
	ALIGN_4

.L26:
	testl	$1, M
	jle	.L30

	movsd	0 * SIZE(A1)     , %xmm0
	movhps	1 * SIZE(A1)     , %xmm0

	movaps	%xmm0, 0 * SIZE(B)
	subl	$-2 * SIZE, B
	ALIGN_4

.L30:
	testl	$1, N
	jle	.L999

	movl	A, A1
	leal	(A, LDA, 2), A2

	movl	M,  I
	sarl	$2, I
	je	.L35
	ALIGN_3

.L32:
	movsd	0 * SIZE(A1)     , %xmm0
	movhps	0 * SIZE(A1, LDA), %xmm0
	movsd	0 * SIZE(A2)     , %xmm1
	movhps	0 * SIZE(A2, LDA), %xmm1

	movaps	%xmm0, 0 * SIZE(B)
	movaps	%xmm1, 2 * SIZE(B)

	leal	(A1, LDA, 4), A1
	leal	(A2, LDA, 4), A2
	subl	$-4 * SIZE, B
	decl	I
	jne	.L32
	ALIGN_3

.L35:
	testl	$2, M
	jle	.L36

	movsd	0 * SIZE(A1)     , %xmm0
	movhps	0 * SIZE(A1, LDA), %xmm0

	movaps	%xmm0, 0 * SIZE(B)

	leal	(A1, LDA, 2), A1
	subl	$-2 * SIZE, B
	ALIGN_4

.L36:
	testl	$1, M
	jle	.L999

	movsd	0 * SIZE(A1)     , %xmm0
	movsd	%xmm0, 0 * SIZE(B)
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
