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

#ifdef PENTIUM
#define P 32
#endif

#if defined(PENTIUM4) || defined(ATHLON)
#define P (DTB_DEFAULT_ENTRIES / 2)
#endif

#ifndef P
#define P DTB_DEFAULT_ENTRIES
#endif

#define STACK	16
#define ARGS	16

#define PLDA_M	  0 + STACK(%esp)
#define XP	  4 + STACK(%esp)
#define MIN_N	  8 + STACK(%esp)
#define IS	 12 + STACK(%esp)

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)

#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	32 + STACK + ARGS(%esp)
#define A	48 + STACK + ARGS(%esp)
#define LDA	52 + STACK + ARGS(%esp)
#define X	56 + STACK + ARGS(%esp)
#define INCX	60 + STACK + ARGS(%esp)
#define Y	64 + STACK + ARGS(%esp)
#define INCY	68 + STACK + ARGS(%esp)
#define BUFFER	72 + STACK + ARGS(%esp)

	PROLOGUE

	subl	$ARGS, %esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	FLD	ALPHA_I
	FLD	ALPHA_R

	movl	X, %edi

	movl	LDA,  %ebx
	sall	$ZBASE_SHIFT, %ebx

	movl	$0, IS

	movl	M, %ecx
	movl	N, %esi

	test	%ecx, %ecx
	jle	.L79			# goto END
	test	%esi, %esi
	jle	.L79			# goto END

	movl	INCY, %eax
	sall	$ZBASE_SHIFT, %eax
	movl	%eax, INCY

	movl	LDA, %eax
	imull	$P,  %eax		# P * lda
	subl	M   ,%eax		# P * lda - m
	sall	$ZBASE_SHIFT, %eax
	movl	%eax, PLDA_M
	ALIGN_2

.L32:
	movl	IS, %esi
	movl	$P, %edx
	movl	N,   %eax
	subl	%esi,%eax		# n - is
	cmpl	%edx,  %eax
#ifdef PENTIUM
	jle	.L33
	movl	%edx,  %eax
.L33:
#else
	cmovg	%edx,  %eax
#endif

	movl	%eax, MIN_N

	sall	$ZBASE_SHIFT, %esi
	leal	(%edi, %esi, 1), %esi
	movl	%esi, XP

	movl	INCX, %edx
	cmpl	$1,   %edx
	je	.L34			# if incx == 1 goto L34

	movl	BUFFER, %esi
	movl	%esi, XP		# xp = buffer

	sall	$ZBASE_SHIFT, %edx
	sarl	$1,%eax
	jle	.L35
	ALIGN_2

.L36:
	FLD	0 * SIZE(%edi)
	FLD	1 * SIZE(%edi)
	addl	%edx,%edi		# x += incx
	FLD	0 * SIZE(%edi)
	FLD	1 * SIZE(%edi)
	addl	%edx,%edi		# x += incx

	FST	3 * SIZE(%esi)
	FST	2 * SIZE(%esi)
	FST	1 * SIZE(%esi)
	FST	0 * SIZE(%esi)

	addl	$4 * SIZE, %esi		# xp += 4
	decl	%eax
	jg	.L36
	ALIGN_3

.L35:
	movl	MIN_N, %eax
	andl	$1,    %eax
	jle	.L34

	FLD	0 * SIZE(%edi)
	FLD	1 * SIZE(%edi)
	addl	%edx,%edi		# x += incx
	FST	1 * SIZE(%esi)
	FST	0 * SIZE(%esi)
	ALIGN_3

/* Main Routine */
.L34:
	movl	 Y, %ecx		# c_offset
	movl	 M, %ebp		# j = m
	ALIGN_3

.L61:
	movl	A, %edx			# a_offset = a
	fldz
	addl	$2 * SIZE, A		# a++
	fldz
	movl	XP,%esi
	fldz
	movl	MIN_N,%eax
	fldz
	FLD	(%esi)			#  bt1 = *(b_offset + 0)
	sarl	$1, %eax
	jle	.L64
	ALIGN_3

.L65:
#ifdef PENTIUM4
       prefetchnta	16 * SIZE(%esi)
#endif

	FLD	 0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	 1 * SIZE(%edx)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
#ifndef CONJ
	faddp	%st, %st(2)		# ct2 += bt1
#else
	fsubrp	%st, %st(2)		# ct2 -= bt1
#endif
	FLD	 1 * SIZE(%esi)		# bt1  = *(b_offset + 1)

	FLD	 0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FLD	 1 * SIZE(%edx)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
	faddp	%st, %st(4)		# ct4 += bt1
	FLD	 2 * SIZE(%esi)		# bt1  = *(b_offset + 2)

	addl	$2 * SIZE, %esi		# b_offset += 2
	addl	%ebx,  %edx		# a_offset += lda

	FLD	 0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	 1 * SIZE(%edx)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
#ifndef CONJ
	faddp	%st, %st(2)		# ct2 += bt1
#else
	fsubrp	%st, %st(2)		# ct2 -= bt1
#endif
	FLD	 1 * SIZE(%esi)		# bt1  = *(b_offset + 1)

	FLD	 0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FLD	 1 * SIZE(%edx)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
	faddp	%st, %st(4)		# ct4 += bt1
	FLD	 2 * SIZE(%esi)		# bt1  = *(b_offset + 2)

	addl	$2 * SIZE, %esi		# b_offset += 2
	addl	%ebx,  %edx		# a_offset += lda

	decl	%eax
	jg	.L65

.L64:
	movl	MIN_N, %eax
	andl	$1, %eax
	jle	.L70
	ALIGN_2

.L71:
	FLD	 0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	 1 * SIZE(%edx)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
#ifndef CONJ
	faddp	%st, %st(2)		# ct2 += bt1
#else
	fsubrp	%st, %st(2)		# ct2 -= bt1
#endif
	FLD	 1 * SIZE(%esi)		# bt1  = *(b_offset + 1)

	FLD	 0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FLD	 1 * SIZE(%edx)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
	faddp	%st, %st(4)		# ct4 += bt1
	fldz
	ALIGN_2

.L70:
	ffreep	%st(0)

#ifndef XCONJ
#ifndef CONJ
	fsubp	%st, %st(3)
	faddp	%st, %st(1)
#else
	faddp	%st, %st(3)
	faddp	%st, %st(1)
#endif
#else
#ifndef CONJ
	faddp	%st, %st(3)
	fsubp	%st, %st(1)
#else
	fsubp	%st, %st(3)
	fsubp	%st, %st(1)
#endif
#endif

	fld	%st(0)		# ct4 = ct2
	fmul	%st(4)
	fld	%st(2)
	fmul	%st(4)
	fsubp	%st, %st(1)

	movl	INCY,  %eax

	FLD	0 * SIZE(%ecx)
	faddp	 %st, %st(1)
	FST	0 * SIZE(%ecx)

	fmul	%st(2)
	fxch	%st(1)
	fmul	%st(3)
	faddp	%st, %st(1)

	FLD	1 * SIZE(%ecx)
	faddp	 %st, %st(1)
	FST	1 * SIZE(%ecx)

	addl	%eax, %ecx
	decl	%ebp
	jg	.L61

.L60:
	movl	PLDA_M, %esi
	addl	%esi, A		# a += P * lda - m
	addl	$P, IS
	movl	N, %esi
	cmpl	%esi,IS
	jl	.L32

.L79:
	ffreep	%st(0)
	ffreep	%st(0)

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
