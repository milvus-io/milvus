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
#define KK	 4 + STACK(%esp)
#define KKK	 8 + STACK(%esp)
#define AORIG	12 + STACK(%esp)

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#define ALPHA	16 + STACK + ARGS(%esp)
#ifdef DOUBLE
#define A	24 + STACK + ARGS(%esp)
#define B	28 + STACK + ARGS(%esp)
#define C	32 + STACK + ARGS(%esp)
#define LDC	36 + STACK + ARGS(%esp)
#define OFFSET	40 + STACK + ARGS(%esp)
#else
#define A	20 + STACK + ARGS(%esp)
#define B	24 + STACK + ARGS(%esp)
#define C	28 + STACK + ARGS(%esp)
#define LDC	32 + STACK + ARGS(%esp)
#define OFFSET	36 + STACK + ARGS(%esp)
#endif

#define PREFETCH_OFFSET 48

#if defined(PENTIUM3) || defined(PENTIUMM)
#define REP rep
#else
#define REP rep
#endif

#define AA	%edx
#define BB	%ecx

	PROLOGUE

	subl	$ARGS, %esp	# Generate Stack Frame

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	LDC, %ebp		# ldc			# MEMORY
	movl	B,   %ebx
	leal	(, %ebp, SIZE), %ebp

#ifdef LN
       movl	M, %eax
       leal	(, %eax, SIZE), %eax
       addl	%eax, C
       imull	K, %eax
       addl	%eax, A
#endif

#ifdef RT
       movl	N, %eax
       leal	(, %eax, SIZE), %eax
       imull	K, %eax
       addl	%eax, %ebx

       movl	N,    %eax
       imull	%ebp, %eax
       addl	%eax, C
#endif

#ifdef RN
	negl	KK
#endif

#ifdef RT
       movl	N, %eax
       subl	OFFSET, %eax
       movl	%eax, KK
#endif

	movl	N,   %eax		# j = (n >> 1)		# MEMORY
	sarl	$1,  %eax
	movl	%eax, J			# j = (n >> 1)		# MEMORY
	je	.L8
	ALIGN_4

.L34:
#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	subl	%eax, %ebx
#endif
	lea	(, %ebp, 2), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, %edi
#ifndef RT
	addl	%eax, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	 M, %eax		# m			# MEMORY
	andl	$1, %eax
	je	.L12

#ifdef LN
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	movl	AORIG, AA
	leal	(AA,   %eax, 1), AA
	leal	(%ebx, %eax, 2), BB
#else
	movl	%ebx, BB
#endif

	fldz
	fldz

	FLD	0 * SIZE(AA)		# temp1 = *(aoffset + 0)

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$1,%eax			# k >> 1		# MEMORY
	je	 .L54
	ALIGN_4

.L55:
	FLD	0 * SIZE(BB)		# temp2 = *(boffset + 0)
	rep
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	1 * SIZE(BB)		# temp2 = *(boffset + 0)
	faddp	%st, %st(2)
	FLD	1 * SIZE(AA)		# temp1 = *(aoffset + 0)

	FLD	2 * SIZE(BB)		# temp2 = *(boffset + 0)
	rep
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	3 * SIZE(BB)		# temp2 = *(boffset + 0)
	faddp	%st, %st(2)
	FLD	2 * SIZE(AA)		# temp1 = *(aoffset + 0)

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jne	.L55
	ALIGN_4

.L54:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$1,%eax			# k & 1
	je	.L33
	ALIGN_4

	FLD	0 * SIZE(BB)		# temp2 = *(boffset + 0)
	rep
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	1 * SIZE(BB)		# temp2 = *(boffset + 0)
	faddp	%st, %st(2)
	FLD	1 * SIZE(AA)		# temp1 = *(aoffset + 0)

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	ALIGN_4

.L33:
	ffreep	%st(0)

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	leal	(, %eax, SIZE), %eax

	movl	AORIG, AA
	leal	(AA,   %eax, 1), AA
	leal	(%ebx, %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	FLD	0 * SIZE(BB)
	fsubp	%st, %st(1)
	FLD	1 * SIZE(BB)
	fsubp	%st, %st(2)
#else
	FLD	0 * SIZE(AA)
	fsubp	%st, %st(1)
	FLD	1 * SIZE(AA)
	fsubp	%st, %st(2)
#endif

#if defined(LN) || defined(LT)
       FLD	0 * SIZE(AA)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef RN
       FLD	0 * SIZE(BB)
       fmulp	%st, %st(1)

       FLD	1 * SIZE(BB)
       fmul	%st(1), %st

       fsubrp	%st, %st(2)

       FLD	3 * SIZE(BB)
       fmulp	%st, %st(2)
#endif

#ifdef RT
       FLD	3 * SIZE(BB)
       fmulp	%st, %st(2)

       FLD	2 * SIZE(BB)
       fmul	%st(2), %st

       fsubrp	%st, %st(1)

       FLD	0 * SIZE(BB)
       fmulp	%st, %st(1)
#endif

#ifdef LN
	subl	$1 * SIZE, %edi
#endif

#if defined(LN) || defined(LT)
	FSTU	0 * SIZE(BB)
	fxch	%st(1)
	FSTU	1 * SIZE(BB)
#else
	FSTU	0 * SIZE(AA)
	fxch	%st(1)
	FSTU	1 * SIZE(AA)
#endif

	FST	0 * SIZE(%edi,%ebp)
	FST	0 * SIZE(%edi)

#ifndef LN
	addl	$1 * SIZE, %edi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif
	ALIGN_4

.L12:
	movl	M,  %esi
	sarl	$1, %esi
	je	.L27
	ALIGN_4

.MainHead:
#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	movl	AORIG, AA
	leal	(AA,   %eax, 2), AA
	leal	(%ebx, %eax, 2), BB
#else
	movl	%ebx, BB
#endif

	fldz
	fldz
	fldz
	fldz

	FLD	 4 * SIZE(BB)		# b5
	FLD	 4 * SIZE(AA)		# a5
	FLD	 0 * SIZE(BB)		# b1
	FLD	 0 * SIZE(AA)		# a1

#ifdef LN
#if   defined(HAVE_3DNOW)
	prefetchw	-2 * SIZE(%edi)
 	prefetchw	-2 * SIZE(%edi, %ebp, 1)
#elif defined(HAVE_SSE)
	prefetchnta	-2 * SIZE(%edi)
 	prefetchnta	-2 * SIZE(%edi, %ebp, 1)
#endif
#else
#if   defined(HAVE_3DNOW)
	prefetchw	2 * SIZE(%edi)
 	prefetchw	2 * SIZE(%edi, %ebp, 1)
#elif defined(HAVE_SSE)
	prefetchnta	2 * SIZE(%edi)
 	prefetchnta	2 * SIZE(%edi, %ebp, 1)
#endif
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
 	je	.L16
	ALIGN_4

.MainLoop:
#if defined(HAVE_3DNOW)
	prefetch	(PREFETCH_OFFSET) * SIZE(BB)
	nop
#elif defined(HAVE_SSE)
	prefetchnta	(PREFETCH_OFFSET) * SIZE(BB)
#if (L2_SIZE == 524288)
	prefetcht0	(PREFETCH_OFFSET) * SIZE(AA)
#endif
#endif

	fmul	%st, %st(1)
	FMUL	 1 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(4)
	FLD	 0 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 1 * SIZE(AA)
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(6)
	FLD	 2 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(7)
	FLD	 2 * SIZE(AA)

	fmul	%st, %st(1)
	FMUL	 3 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(4)
	FLD	 2 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 3 * SIZE(AA)
	fmul	%st, %st(1)
	FMUL	 3 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(6)
	FLD	 8 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(7)
	FLD	 8 * SIZE(AA)
	fxch	%st(2)

#if !defined(HAVE_3DNOW) && defined(HAVE_SSE)  && defined(DOUBLE)
	prefetchnta	(PREFETCH_OFFSET + 4) * SIZE(BB)
#if (L2_SIZE == 524288)
	prefetcht0	(PREFETCH_OFFSET + 4) * SIZE(AA)
#endif
#endif

	fmul	%st, %st(3)
	FMUL	 5 * SIZE(BB)
	fxch	%st(3)
	faddp	%st, %st(4)
	FLD	 4 * SIZE(BB)
	fxch	%st(3)
	faddp	%st, %st(5)
	FLD	 5 * SIZE(AA)
	fmul	%st, %st(3)
	FMUL	 5 * SIZE(BB)
	fxch	%st(3)
	faddp	%st, %st(6)
	FLD	 6 * SIZE(BB)
	fxch	%st(3)
	faddp	%st, %st(7)
	FLD	 6 * SIZE(AA)

	fmul	%st, %st(3)
	FMUL	 7 * SIZE(BB)
	fxch	%st(3)
	faddp	%st, %st(4)
	FLD	 6 * SIZE(BB)
	fxch	%st(3)
	faddp	%st, %st(5)
	FLD	 7 * SIZE(AA)
	fmul	%st, %st(3)
	FMUL	 7 * SIZE(BB)
	fxch	%st(3)
	faddp	%st, %st(6)
	FLD	12 * SIZE(BB)
	fxch	%st(3)
	faddp	%st, %st(7)
	FLD	12 * SIZE(AA)
	fxch	%st(2)

	subl	$-8 * SIZE, BB
	subl	$-8 * SIZE, AA
	decl	%eax			# l --
	jne	.MainLoop
	ALIGN_4

.L16:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	and	$3,  %eax
	je	.L21
	ALIGN_4

.SubLoop:
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(4)
	FLD	 0 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 1 * SIZE(AA)
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(6)
	FLD	 2 * SIZE(BB)
	fxch	%st(1)
	faddp	%st, %st(7)
	FLD	 2 * SIZE(AA)

	addl	$2 * SIZE,BB
	addl	$2 * SIZE,AA
	decl	%eax
	jne	 .SubLoop
	ALIGN_4

.L21:
	ffreep	%st(0)
	ffreep	%st(0)
	ffreep	%st(0)
	ffreep	%st(0)

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$2, %eax
#endif

	leal	(, %eax, SIZE), %eax

	movl	AORIG, AA
	leal	(AA,   %eax, 2), AA
	leal	(%ebx, %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	FLD	0 * SIZE(BB)
	fsubp	%st, %st(1)
	FLD	1 * SIZE(BB)
	fsubp	%st, %st(2)
	FLD	2 * SIZE(BB)
	fsubp	%st, %st(3)
	FLD	3 * SIZE(BB)
	fsubp	%st, %st(4)
#else
	FLD	0 * SIZE(AA)
	fsubp	%st, %st(1)
	FLD	1 * SIZE(AA)
	fsubp	%st, %st(3)
	FLD	2 * SIZE(AA)
	fsubp	%st, %st(2)
	FLD	3 * SIZE(AA)
	fsubp	%st, %st(4)
#endif

#ifdef LN
       FLD	3 * SIZE(AA)
       fmul	%st, %st(3)
       fmulp	%st, %st(4)

       FLD	2 * SIZE(AA)
       fmul	%st(3), %st
       FLD	2 * SIZE(AA)
       fmul	%st(5), %st

       fsubrp	%st, %st(3)
       fsubrp	%st, %st(1)

       FLD	0 * SIZE(AA)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef LT
       FLD	0 * SIZE(AA)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)

       FLD	1 * SIZE(AA)
       fmul	%st(1), %st
       FLD	1 * SIZE(AA)
       fmul	%st(3), %st

       fsubrp	%st, %st(5)
       fsubrp	%st, %st(3)

       FLD	3 * SIZE(AA)
       fmul	%st, %st(3)
       fmulp	%st, %st(4)
#endif

#ifdef RN
       FLD	0 * SIZE(BB)
       fmul	%st, %st(1)
       fmulp	%st, %st(3)

       FLD	1 * SIZE(BB)
       fmul	%st(1), %st
       FLD	1 * SIZE(BB)
       fmul	%st(4), %st

       fsubrp	%st, %st(5)
       fsubrp	%st, %st(2)

       FLD	3 * SIZE(BB)
       fmul	%st, %st(2)
       fmulp	%st, %st(4)
#endif

#ifdef RT
       FLD	3 * SIZE(BB)
       fmul	%st, %st(2)
       fmulp	%st, %st(4)

       FLD	2 * SIZE(BB)
       fmul	%st(2), %st
       FLD	2 * SIZE(BB)
       fmul	%st(5), %st

       fsubrp	%st, %st(4)
       fsubrp	%st, %st(1)

       FLD	0 * SIZE(BB)
       fmul	%st, %st(1)
       fmulp	%st, %st(3)
#endif

#ifdef LN
	subl	$2 * SIZE, %edi
#endif

#if defined(LN) || defined(LT)
	FSTU	0 * SIZE(BB)
	fxch	%st(1)
	FSTU	1 * SIZE(BB)
	fxch	%st(2)
	FSTU	2 * SIZE(BB)
	fxch	%st(3)
	FSTU	3 * SIZE(BB)

	FST	1 * SIZE(%edi,%ebp)
	FST	0 * SIZE(%edi)
	FST	0 * SIZE(%edi,%ebp)
	FST	1 * SIZE(%edi)
#else
	FSTU	0 * SIZE(AA)
	fxch	%st(2)
	FSTU	1 * SIZE(AA)
	fxch	%st(1)
	FSTU	2 * SIZE(AA)
	fxch	%st(3)
	FSTU	3 * SIZE(AA)

	FST	1 * SIZE(%edi,%ebp)
	FST	1 * SIZE(%edi)
	FST	0 * SIZE(%edi)
	FST	0 * SIZE(%edi,%ebp)
#endif

#ifndef LN
	addl	$2 * SIZE, %edi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif

	decl	%esi			# i --
	jne	.MainHead
	ALIGN_4

.L27:
#ifdef LN
       movl	K, %eax
       leal	(    , %eax, SIZE), %eax
       leal	(%ebx, %eax, 2), %ebx
#endif
#if defined(LT) || defined(RN)
	movl	BB, %ebx
#endif

#ifdef RN
	addl	$2, KK
#endif

#ifdef RT
	subl	$2, KK
#endif

	decl	J			# j--			# MEMORY
	jne	.L34
	ALIGN_4

.L8:
	movl	N,  %eax		# n			# MEMORY
	andl	$1, %eax
	je	.End

#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$0 + BASE_SHIFT, %eax
	subl	%eax, %ebx
#endif

#ifdef RT
	subl	%ebp, C
#endif
	movl	C, %edi			# c			# MEMORY
#ifndef RT
	addl	%ebp, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	M,  %eax		# m			# MEMORY
	andl	$1, %eax		# m & 1
	je	.L36

#ifdef LN
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	movl	AORIG, AA
	leal	(AA,   %eax, 1), AA
	leal	(%ebx, %eax, 1), BB
#else
	movl	%ebx, BB
#endif

	fldz

#ifdef LN
#if   defined(HAVE_3DNOW)
	prefetchw	-2 * SIZE(%edi)
#elif defined(HAVE_SSE)
	prefetchnta	-2 * SIZE(%edi)
#endif
#else
#if   defined(HAVE_3DNOW)
	prefetchw	2 * SIZE(%edi)
#elif defined(HAVE_SSE)
	prefetchnta	2 * SIZE(%edi)
#endif
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	test	%eax, %eax
	jle	.L52
	ALIGN_3

.L51:
	FLD	(AA)
	FMUL	(BB)
	addl	$1 * SIZE,AA
	addl	$1 * SIZE,BB
	faddp	%st,%st(1)
	decl	%eax
	jne	.L51
	ALIGN_4

.L52:

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	leal	(, %eax, SIZE), %eax

	movl	AORIG, AA
	leal	(AA,   %eax, 1), AA
	leal	(%ebx, %eax, 1), BB
#endif

#if defined(LN) || defined(LT)
	FLD	0 * SIZE(BB)
	fsubp	%st, %st(1)
#else
	FLD	0 * SIZE(AA)
	fsubp	%st, %st(1)
#endif

#if defined(LN) || defined(LT)
       FMUL	0 * SIZE(AA)
#else
       FMUL	0 * SIZE(BB)
#endif

#ifdef LN
	subl	$1 * SIZE, %edi
#endif

#if defined(LN) || defined(LT)
	FSTU	0 * SIZE(BB)
#else
	FSTU	0 * SIZE(AA)
#endif

	FST	0 * SIZE(%edi)

#ifndef LN
	addl	$1 * SIZE, %edi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 1), BB
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif
	ALIGN_4

.L36:
	movl	M,  %esi		# m			# MEMORY
	sarl	$1, %esi		# m >> 1
	je	.L99
	ALIGN_4

.L46:
#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	movl	AORIG, AA
	leal	(AA,   %eax, 2), AA
	leal	(%ebx, %eax, 1), BB
#else
	movl	%ebx, BB
#endif

	fldz
	fldz
	FLD	0 * SIZE(BB)		# temp1 = *(boffset + 0)

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$1, %eax
	je	.L56
	ALIGN_4

.L57:
	FLD	0 * SIZE(AA)		# temp2 = *(aoffset + 0)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	1 * SIZE(AA)		# temp2 = *(aoffset + 0)
	faddp	%st, %st(2)
	FLD	1 * SIZE(BB)		# temp1 = *(boffset + 0)

	FLD	2 * SIZE(AA)		# temp2 = *(aoffset + 0)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	3 * SIZE(AA)		# temp2 = *(aoffset + 0)
	faddp	%st, %st(2)
	FLD	2 * SIZE(BB)		# temp1 = *(boffset + 0)

	addl	$4 * SIZE,AA
	addl	$2 * SIZE,BB
	dec	%eax
	jne	.L57
	ALIGN_4

.L56:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$1, %eax
	je	.L45
	ALIGN_4

	FLD	0 * SIZE(AA)		# temp2 = *(aoffset + 0)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	1 * SIZE(AA)		# temp2 = *(aoffset + 0)
	faddp	%st, %st(2)
	FLD	3 * SIZE(BB)		# temp1 = *(boffset + 0)

	addl	$2 * SIZE,AA
	addl	$1 * SIZE,BB
	ALIGN_4

.L45:
	ffreep	%st(0)

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$1, %eax
#endif

	leal	(, %eax, SIZE), %eax

	movl	AORIG, AA
	leal	(AA,   %eax, 2), AA
	leal	(%ebx, %eax, 1), BB
#endif

#if defined(LN) || defined(LT)
	FLD	0 * SIZE(BB)
	fsubp	%st, %st(1)
	FLD	1 * SIZE(BB)
	fsubp	%st, %st(2)
#else
	FLD	0 * SIZE(AA)
	fsubp	%st, %st(1)
	FLD	1 * SIZE(AA)
	fsubp	%st, %st(2)
#endif

#ifdef LN
       FLD	3 * SIZE(AA)
       fmulp	%st, %st(2)

       FLD	2 * SIZE(AA)
       fmul	%st(2), %st

       fsubrp	%st, %st(1)
       FLD	0 * SIZE(AA)
       fmulp	%st, %st(1)
#endif

#ifdef LT
       FLD	0 * SIZE(AA)
       fmulp	%st, %st(1)

       FLD	1 * SIZE(AA)
       fmul	%st(1), %st

       fsubrp	%st, %st(2)

       FLD	3 * SIZE(AA)
       fmulp	%st, %st(2)
#endif

#ifdef RN
       FLD	0 * SIZE(BB)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef RT
       FLD	0 * SIZE(BB)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef LN
	subl	$2 * SIZE, %edi
#endif

#if defined(LN) || defined(LT)
	FSTU	0 * SIZE(BB)
	fxch	%st(1)
	FSTU	1 * SIZE(BB)
#else
	FSTU	0 * SIZE(AA)
	fxch	%st(1)
	FSTU	1 * SIZE(AA)
#endif

	FST	1 * SIZE(%edi)
	FST	0 * SIZE(%edi)

#ifndef LN
	addl	$2 * SIZE, %edi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 1), BB
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif

	decl	%esi			# i --
	jne	.L46
	ALIGN_4

.L99:
#ifdef LN
       movl	K, %eax
       leal	(%ebx, %eax, SIZE), %ebx
#endif
#if defined(LT) || defined(RN)
	movl	BB, %ebx
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif
	ALIGN_4


.End:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
