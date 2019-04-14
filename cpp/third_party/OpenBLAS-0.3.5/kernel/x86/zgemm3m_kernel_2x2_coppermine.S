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

#define J	 0 + STACK(%esp)
#define BX	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#ifdef DOUBLE
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	24 + STACK + ARGS(%esp)
#define A	32 + STACK + ARGS(%esp)
#define B	36 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define LDC	44 + STACK + ARGS(%esp)
#else
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	20 + STACK + ARGS(%esp)
#define A	24 + STACK + ARGS(%esp)
#define B	28 + STACK + ARGS(%esp)
#define C	32 + STACK + ARGS(%esp)
#define LDC	36 + STACK + ARGS(%esp)
#endif

#define PREFETCH_OFFSET 48

#if defined(PENTIUM3) || defined(PENTIUMM)
#define REP rep
#else
#define REP rep
#endif

	PROLOGUE

	subl	$ARGS, %esp	# Generate Stack Frame

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(TRMMKERNEL) && !defined(LEFT)
	movl	OFFSET, %eax
	negl	%eax
	movl	%eax, KK
#endif

	movl	N,   %eax		# j = (n >> 1)		# MEMORY
	movl	LDC, %ebp		# ldc			# MEMORY
	movl	B,   %ebx

	sall	$ZBASE_SHIFT, %ebp

	sarl	$1,  %eax

	leal	0(%ecx) , %ecx		# NOP
	movl	%eax, J			# j = (n >> 1)		# MEMORY
	test	%eax, %eax
	je	.L8			# if !(n >> 1) goto .L8
	ALIGN_4

.L34:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	%ebx, BX

	movl	M, %esi			# m			# MEMORY
	movl	A, %edx			# a			# MEMORY
	movl	C, %edi			# C			# MEMORY
	sarl	$1,   %esi		# i = (m >> 1)
	je	.L12
	ALIGN_4

.MainHead:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	%ebx, %ecx
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(%edx, %eax, 2), %edx
	leal	(%ebx, %eax, 2), %ecx
#endif

#ifdef HAVE_SSE
	movl	BX, %eax

	prefetcht2  0 * SIZE(%eax)
	prefetcht2  4 * SIZE(%eax)

#if   L2_SIZE > 262144

	subl	$-8 * SIZE, BX

#elif L2_SIZE > 131072

	prefetcht2  8 * SIZE(%eax)
	prefetcht2 12 * SIZE(%eax)


	subl	$-16 * SIZE, BX
#else
	prefetcht2 16 * SIZE(%eax)
	prefetcht2 20 * SIZE(%eax)
	prefetcht2 24 * SIZE(%eax)
	prefetcht2 28 * SIZE(%eax)

	subl	$-32 * SIZE, BX
#endif
#endif

	fldz
	fldz

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
	fldz
	fldz

	FLD	 4 * SIZE(%ecx)		# b5
	FLD	 4 * SIZE(%edx)		# a5
	FLD	 0 * SIZE(%ecx)		# b1
	FLD	 0 * SIZE(%edx)		# a1

#if   defined(HAVE_3DNOW)
	prefetchw	2 * SIZE(%edi)
 	prefetchw	2 * SIZE(%edi, %ebp, 1)
#elif defined(HAVE_SSE)
	prefetchnta	2 * SIZE(%edi)
 	prefetchnta	2 * SIZE(%edi, %ebp, 1)
#endif
	sarl	$2, %eax
 	je	.L16
	ALIGN_4

.MainLoop:
#if defined(HAVE_3DNOW)
	prefetch	(PREFETCH_OFFSET) * SIZE(%ecx)
	nop
#elif defined(HAVE_SSE)
	prefetchnta	(PREFETCH_OFFSET) * SIZE(%ecx)
#ifdef CORE_KATMAI
	prefetcht0	(PREFETCH_OFFSET) * SIZE(%edx)
#endif
#endif

	fmul	%st, %st(1)
	FMUL	 1 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(4)
	FLD	 0 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 1 * SIZE(%edx)
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(6)
	FLD	 2 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(7)
	FLD	 2 * SIZE(%edx)

	fmul	%st, %st(1)
	FMUL	 3 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(4)
	FLD	 2 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 3 * SIZE(%edx)
	fmul	%st, %st(1)
	FMUL	 3 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(6)
	FLD	 8 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(7)
	FLD	 8 * SIZE(%edx)
	fxch	%st(2)

#if !defined(HAVE_3DNOW) && defined(HAVE_SSE)  && defined(DOUBLE)
	prefetchnta	(PREFETCH_OFFSET + 4) * SIZE(%ecx)
#ifdef CORE_KATMAI
	prefetcht0	(PREFETCH_OFFSET + 4) * SIZE(%edx)
#endif
#endif

	fmul	%st, %st(3)
	FMUL	 5 * SIZE(%ecx)
	fxch	%st(3)
	faddp	%st, %st(4)
	FLD	 4 * SIZE(%ecx)
	fxch	%st(3)
	faddp	%st, %st(5)
	FLD	 5 * SIZE(%edx)
	fmul	%st, %st(3)
	FMUL	 5 * SIZE(%ecx)
	fxch	%st(3)
	faddp	%st, %st(6)
	FLD	 6 * SIZE(%ecx)
	fxch	%st(3)
	faddp	%st, %st(7)
	FLD	 6 * SIZE(%edx)

	fmul	%st, %st(3)
	FMUL	 7 * SIZE(%ecx)
	fxch	%st(3)
	faddp	%st, %st(4)
	FLD	 6 * SIZE(%ecx)
	fxch	%st(3)
	faddp	%st, %st(5)
	FLD	 7 * SIZE(%edx)
	fmul	%st, %st(3)
	FMUL	 7 * SIZE(%ecx)
	fxch	%st(3)
	faddp	%st, %st(6)
	FLD	12 * SIZE(%ecx)
	fxch	%st(3)
	faddp	%st, %st(7)
	FLD	12 * SIZE(%edx)
	fxch	%st(2)

	subl	$-8 * SIZE, %ecx
	subl	$-8 * SIZE, %edx
	decl	%eax			# l --
	jne	.MainLoop
	ALIGN_4

.L16:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	and	$3,  %eax
	je	.L21
	ALIGN_4

.SubLoop:
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(4)
	FLD	 0 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 1 * SIZE(%edx)
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(6)
	FLD	 2 * SIZE(%ecx)
	fxch	%st(1)
	faddp	%st, %st(7)
	FLD	 2 * SIZE(%edx)

	addl	$2 * SIZE,%ecx
	addl	$2 * SIZE,%edx
	decl	%eax
	jne	 .SubLoop
	ALIGN_4

.L21:
	ffreep	%st(0)
	ffreep	%st(0)
	ffreep	%st(0)
	ffreep	%st(0)

	FLD	ALPHA_I
	FLD	ALPHA_R

	fld	%st(2)
	fmul	%st(1), %st

	FLD	0 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi)

	fld	%st(3)
	fmul	%st(1), %st

	FLD	0 * SIZE(%edi, %ebp)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi, %ebp)

	fld	%st(4)
	fmul	%st(1), %st

	FLD	2 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	2 * SIZE(%edi)

	fmul	%st(5), %st

	FLD	2 * SIZE(%edi, %ebp)
	faddp	%st, %st(1)
	FST	2 * SIZE(%edi, %ebp)

	fmul	%st, %st(1)
	fmul	%st, %st(2)
	fmul	%st, %st(3)
	fmulp	%st, %st(4)

	FLD	1 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi)

	FLD	1 * SIZE(%edi, %ebp)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi, %ebp)

	FLD	3 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	3 * SIZE(%edi)

	FLD	3 * SIZE(%edi, %ebp)
	faddp	%st, %st(1)
	FST	3 * SIZE(%edi, %ebp)

	addl	$4 * SIZE, %edi
	rep
	decl	%esi			# i --
	rep
	jne	.MainHead
	ALIGN_4

.L12:
	movl	 M, %eax		# m			# MEMORY
	andl	$1, %eax
	je	.L27

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	%ebx, %ecx
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(%edx, %eax, 1), %edx
	leal	(%ebx, %eax, 2), %ecx
#endif
	fldz
	fldz

	FLD	0 * SIZE(%edx)		# temp1 = *(aoffset + 0)

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
	sarl	$1,%eax			# k >> 1		# MEMORY
	je	 .L54
	ALIGN_4

.L55:
	FLD	0 * SIZE(%ecx)		# temp2 = *(boffset + 0)
	rep
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	1 * SIZE(%ecx)		# temp2 = *(boffset + 0)
	faddp	%st, %st(2)
	FLD	1 * SIZE(%edx)		# temp1 = *(aoffset + 0)

	FLD	2 * SIZE(%ecx)		# temp2 = *(boffset + 0)
	rep
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	3 * SIZE(%ecx)		# temp2 = *(boffset + 0)
	faddp	%st, %st(2)
	FLD	2 * SIZE(%edx)		# temp1 = *(aoffset + 0)

	addl	$2 * SIZE, %edx
	addl	$4 * SIZE, %ecx
	decl	%eax
	jne	.L55
	ALIGN_4

.L54:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$1,%eax			# k & 1
	je	.L33
	ALIGN_4

	FLD	0 * SIZE(%ecx)		# temp2 = *(boffset + 0)
	rep
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	1 * SIZE(%ecx)		# temp2 = *(boffset + 0)
	faddp	%st, %st(2)
	FLD	1 * SIZE(%edx)		# temp1 = *(aoffset + 0)

	addl	$1 * SIZE, %edx
	addl	$2 * SIZE, %ecx
	ALIGN_4

.L33:
	ffreep	%st(0)

	FLD	ALPHA_I
	FLD	ALPHA_R

	fld	%st(2)
	fmul	%st(1), %st

	FLD	0 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi)

	fmul	%st(3), %st

	FLD	0 * SIZE(%edi, %ebp)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi, %ebp)

	fmul	%st, %st(1)
	fmulp	%st, %st(2)

	FLD	1 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi)

	FLD	1 * SIZE(%edi, %ebp)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi, %ebp)
	ALIGN_4

.L27:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	lea	(, %ebp, 2), %eax
	addl	%eax, C			# C + 2 * ldc		# MEMORY
	movl	%ecx, %ebx		# b			# MEMORY
	decl	J			# j--			# MEMORY
	jne	.L34
	ALIGN_4

.L8:
	movl	N,  %eax		# n			# MEMORY
	andl	$1, %eax
	je	.End

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	C, %edi			# c			# MEMORY
	movl	A, %edx			# a			# MEMORY

	movl	M,  %esi		# m			# MEMORY
	sarl	$1, %esi		# m >> 1
	je	.L36
	ALIGN_4

.L46:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	%ebx, %ecx
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(%edx, %eax, 2), %edx
	leal	(%ebx, %eax, 1), %ecx
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
	fldz
	sarl	$1, %eax
	fldz
	FLD	0 * SIZE(%ecx)		# temp1 = *(boffset + 0)

	je	.L56
	ALIGN_4

.L57:
	FLD	0 * SIZE(%edx)		# temp2 = *(aoffset + 0)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	1 * SIZE(%edx)		# temp2 = *(aoffset + 0)
	faddp	%st, %st(2)
	FLD	1 * SIZE(%ecx)		# temp1 = *(boffset + 0)

	FLD	2 * SIZE(%edx)		# temp2 = *(aoffset + 0)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	3 * SIZE(%edx)		# temp2 = *(aoffset + 0)
	faddp	%st, %st(2)
	FLD	2 * SIZE(%ecx)		# temp1 = *(boffset + 0)

	addl	$4 * SIZE,%edx
	addl	$2 * SIZE,%ecx
	dec	%eax
	jne	.L57
	ALIGN_4

.L56:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$1, %eax
	je	.L45
	ALIGN_4

	FLD	0 * SIZE(%edx)		# temp2 = *(aoffset + 0)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	1 * SIZE(%edx)		# temp2 = *(aoffset + 0)
	faddp	%st, %st(2)
	FLD	3 * SIZE(%ecx)		# temp1 = *(boffset + 0)

	addl	$2 * SIZE,%edx
	addl	$1 * SIZE,%ecx
	ALIGN_4

.L45:
	ffreep	%st(0)

	FLD	ALPHA_I
	FLD	ALPHA_R

	fld	%st(2)
	fmul	%st(1), %st

	FLD	0 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi)

	fmul	%st(3), %st

	FLD	2 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	2 * SIZE(%edi)

	fmul	%st, %st(1)
	fmulp	%st, %st(2)

	FLD	1 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi)

	FLD	3 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	3 * SIZE(%edi)

	addl	$4 * SIZE, %edi

	decl	%esi			# i --
	jne	.L46
	ALIGN_4

.L36:
	movl	M,  %eax		# m			# MEMORY
	andl	$1, %eax		# m & 1
	je	.End

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	%ebx, %ecx
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(%edx, %eax, 1), %edx
	leal	(%ebx, %eax, 1), %ecx
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
	fldz
	ALIGN_3

.L51:
	FLD	(%edx)
	FMUL	(%ecx)
	addl	$1 * SIZE,%edx
	addl	$1 * SIZE,%ecx
	faddp	%st,%st(1)
	decl	%eax
	jne	.L51

	FLD	ALPHA_I
	FLD	ALPHA_R

	fmul	%st(2), %st

	FLD	0 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi)

	fmulp	%st, %st(1)

	FLD	1 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi)
	ALIGN_4

.End:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
