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

#define RPREFETCHSIZE	12
#define WPREFETCHSIZE (RPREFETCHSIZE * 2)
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht2

#define STACK	16
#define ARGS	 8

#define J	 0 + STACK(%esp)
#define BOFFSET2 4 + STACK(%esp)

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define A	12 + STACK + ARGS(%esp)
#define LDA	16 + STACK + ARGS(%esp)
#define B	20 + STACK + ARGS(%esp)

	PROLOGUE

	subl	$ARGS, %esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	A, %ebp
	movl	B, %edi

	movl	M,   %ebx
	movl	N,   %eax
	andl	$-2, %eax

	imull	%ebx, %eax		# m * ( n & ~1)

	leal	(%edi,%eax,SIZE), %eax	# boffset2 = b + m * (n & ~1)
	movl	%eax, BOFFSET2

	movl	M, %esi
#ifdef DOUBLE
	sall	$4,%esi
#else
	sall	$3,%esi
#endif

	sarl	$1,  %ebx		# if !(m & 1) goto L28
	movl	%ebx, J
	jle	.L28
	ALIGN_4

.L39:
	movl	%ebp, %edx		# aoffset1 = a
	movl	LDA,  %eax
	movl	N,    %ebx

	leal	(%ebp, %eax,SIZE), %ecx	# aoffset2 = a + lda
	leal	(%ecx, %eax,SIZE), %ebp	# aoffset += 2 * lda
	movl	%edi, %eax		# boffset1 = b_offset
	addl	$4 * SIZE, %edi		# boffset += 4

	sarl	$2, %ebx
	jle	.L32
	ALIGN_4

.L36:
	PREFETCH	RPREFETCHSIZE * SIZE(%edx)

	movsd	0 * SIZE(%edx), %xmm0
	movhps	1 * SIZE(%edx), %xmm0
	movsd	0 * SIZE(%ecx), %xmm2
	movhps	1 * SIZE(%ecx), %xmm2

	PREFETCH	RPREFETCHSIZE * SIZE(%ecx)

	movsd	2 * SIZE(%edx), %xmm4
	movhps	3 * SIZE(%edx), %xmm4
	movsd	2 * SIZE(%ecx), %xmm6
	movhps	3 * SIZE(%ecx), %xmm6

	movaps	%xmm0, 0 * SIZE(%eax)
	movaps	%xmm2, 2 * SIZE(%eax)

	addl	%esi, %eax

	movaps	%xmm4, 0 * SIZE(%eax)
	movaps	%xmm6, 2 * SIZE(%eax)

	addl	$4 * SIZE, %ecx
	addl	$4 * SIZE, %edx
	addl	%esi, %eax
	decl	%ebx
	jne	.L36
	ALIGN_4

.L32:
	movl	N,  %ebx
	test	$2, %ebx
	je	.L37

	PREFETCH	RPREFETCHSIZE * SIZE(%edx)
	movsd	0 * SIZE(%edx), %xmm0
	movhps	1 * SIZE(%edx), %xmm0

	PREFETCH	RPREFETCHSIZE * SIZE(%ecx)
	movsd	0 * SIZE(%ecx), %xmm2
	movhps	1 * SIZE(%ecx), %xmm2

	movaps	%xmm0, 0 * SIZE(%eax)
	movaps	%xmm2, 2 * SIZE(%eax)

	addl	$2 * SIZE, %ecx
	addl	$2 * SIZE, %edx
	ALIGN_4

.L37:
	movl	N, %ebx
	test	$1, %ebx
	je	.L38

	movl	BOFFSET2, %eax

	movsd	0 * SIZE(%edx), %xmm0
	movhps	0 * SIZE(%ecx), %xmm0
	movaps	%xmm0, 0 * SIZE(%eax)

	addl	$2 * SIZE, %eax
	movl	%eax, BOFFSET2
	ALIGN_4

.L38:
	decl	J
	jg	.L39
	ALIGN_4

.L28:
	movl	M,  %eax
	movl	N, %ebx

	testb	$1, %al
	je	.L40

	sarl	$2, %ebx
	jle	.L41
	ALIGN_4

.L45:
	movsd	0 * SIZE(%ebp), %xmm0
	movhps	1 * SIZE(%ebp), %xmm0
	movsd	2 * SIZE(%ebp), %xmm2
	movhps	3 * SIZE(%ebp), %xmm2

	movaps	%xmm0, 0 * SIZE(%edi)

	addl	%esi, %edi

	movaps  %xmm2, 0 * SIZE(%edi)

	addl	%esi,%edi
	addl	$4 * SIZE, %ebp
	decl	%ebx
	jg	.L45
	ALIGN_4

.L41:
	movl	N,  %ebx
	test	$2, %ebx
	je	.L46

	movsd	0 * SIZE(%ebp), %xmm0
	movhps	1 * SIZE(%ebp), %xmm0
	movaps	%xmm0, 0 * SIZE(%edi)
	addl	$2 * SIZE, %ebp
	ALIGN_4

.L46:
	movl	N,  %ebx
	test	$1, %ebx
	je	.L40

	movl	BOFFSET2, %eax

	movsd	0 * SIZE(%ebp), %xmm0
	movsd	%xmm0, 0 * SIZE(%eax)
	ALIGN_4

.L40:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS,%esp
	ret

	EPILOGUE
