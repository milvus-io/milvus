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

	EMMS

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
#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(%edx), %mm0
	MMXLOAD		1 * SIZE(%edx), %mm1
	MMXLOAD		0 * SIZE(%ecx), %mm2
	MMXLOAD		1 * SIZE(%ecx), %mm3

	MMXLOAD		2 * SIZE(%edx), %mm4
	MMXLOAD		3 * SIZE(%edx), %mm5
	MMXLOAD		2 * SIZE(%ecx), %mm6
	MMXLOAD		3 * SIZE(%ecx), %mm7

	MMXSTORE	%mm0, 0 * SIZE(%eax)
	MMXSTORE	%mm1, 1 * SIZE(%eax)
	MMXSTORE	%mm2, 2 * SIZE(%eax)
	MMXSTORE	%mm3, 3 * SIZE(%eax)

	addl	%esi, %eax

	MMXSTORE	%mm4, 0 * SIZE(%eax)
	MMXSTORE	%mm5, 1 * SIZE(%eax)
	MMXSTORE	%mm6, 2 * SIZE(%eax)
	MMXSTORE	%mm7, 3 * SIZE(%eax)
#else
	FLD	1 * SIZE(%ecx)
	FLD	0 * SIZE(%ecx)
	FLD	1 * SIZE(%edx)
	FLD	0 * SIZE(%edx)

	FST	0 * SIZE(%eax)
	FST	1 * SIZE(%eax)
	FST	2 * SIZE(%eax)
	FST	3 * SIZE(%eax)

	addl	%esi, %eax

	FLD	3 * SIZE(%ecx)
	FLD	2 * SIZE(%ecx)
	FLD	3 * SIZE(%edx)
	FLD	2 * SIZE(%edx)

	FST	0 * SIZE(%eax)
	FST	1 * SIZE(%eax)
	FST	2 * SIZE(%eax)
	FST	3 * SIZE(%eax)
#endif

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

#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(%edx), %mm0
	MMXLOAD		1 * SIZE(%edx), %mm1
	MMXLOAD		0 * SIZE(%ecx), %mm2
	MMXLOAD		1 * SIZE(%ecx), %mm3

	MMXSTORE	%mm0, 0 * SIZE(%eax)
	MMXSTORE	%mm1, 1 * SIZE(%eax)
	MMXSTORE	%mm2, 2 * SIZE(%eax)
	MMXSTORE	%mm3, 3 * SIZE(%eax)
#else
	FLD	1 * SIZE(%ecx)
	FLD	0 * SIZE(%ecx)
	FLD	1 * SIZE(%edx)
	FLD	0 * SIZE(%edx)

	FST	0 * SIZE(%eax)
	FST	1 * SIZE(%eax)
	FST	2 * SIZE(%eax)
	FST	3 * SIZE(%eax)
#endif

	addl	$2 * SIZE, %ecx
	addl	$2 * SIZE, %edx
	ALIGN_4

.L37:
	movl	N, %ebx
	test	$1, %ebx
	je	.L38

	movl	BOFFSET2, %eax

#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(%edx), %mm0
	MMXLOAD		0 * SIZE(%ecx), %mm1
	MMXSTORE	%mm0, 0 * SIZE(%eax)
	MMXSTORE	%mm1, 1 * SIZE(%eax)
#else
	FLD	0 * SIZE(%edx)
	FST	0 * SIZE(%eax)
	FLD	0 * SIZE(%ecx)
	FST	1 * SIZE(%eax)
#endif
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
#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(%ebp), %mm0
	MMXLOAD		1 * SIZE(%ebp), %mm1
	MMXLOAD		2 * SIZE(%ebp), %mm2
	MMXLOAD		3 * SIZE(%ebp), %mm3

	MMXSTORE	%mm0, 0 * SIZE(%edi)
	MMXSTORE	%mm1, 1 * SIZE(%edi)

	addl	%esi, %edi

	MMXSTORE      %mm2, 0 * SIZE(%edi)
	MMXSTORE      %mm3, 1 * SIZE(%edi)
#else
	FLD	0 * SIZE(%ebp)
	FST	0 * SIZE(%edi)
	FLD	1 * SIZE(%ebp)
	FST	1 * SIZE(%edi)
	addl	%esi, %edi

	FLD	2 * SIZE(%ebp)
	FST	0 * SIZE(%edi)
	FLD	3 * SIZE(%ebp)
	FST	1 * SIZE(%edi)
#endif
	addl	%esi,%edi
	addl	$4 * SIZE, %ebp
	decl	%ebx
	jg	.L45
	ALIGN_4

.L41:
	movl	N,  %ebx
	test	$2, %ebx
	je	.L46

#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(%ebp), %mm0
	MMXSTORE	%mm0, 0 * SIZE(%edi)
	MMXLOAD		1 * SIZE(%ebp), %mm1
	MMXSTORE	%mm1, 1 * SIZE(%edi)
#else
	FLD	1 * SIZE(%ebp)
	FLD	0 * SIZE(%ebp)
	FST	0 * SIZE(%edi)
	FST	1 * SIZE(%edi)
#endif

	addl	$2 * SIZE, %ebp
	ALIGN_4

.L46:
	movl	N,  %ebx
	test	$1, %ebx
	je	.L40

	movl	BOFFSET2, %eax

#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(%ebp), %mm0
	MMXSTORE	%mm0, 0 * SIZE(%eax)
#else
	FLD	(%ebp)
	FST	(%eax)
#endif
	ALIGN_4

.L40:
	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS,%esp
	ret

	EPILOGUE
