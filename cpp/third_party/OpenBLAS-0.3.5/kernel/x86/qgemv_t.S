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
#define P 88
#endif

#ifndef P
#define P 1000
#endif

#define STACK	16
#define ARGS	24

#define NLDA	  0 + STACK(%esp)
#define XP	  4 + STACK(%esp)
#define MIN_M	  8 + STACK(%esp)
#define J	 12 + STACK(%esp)
#define IS	 16 + STACK(%esp)

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#define ALPHA	16 + STACK + ARGS(%esp)

#define A	32 + STACK + ARGS(%esp)
#define LDA	36 + STACK + ARGS(%esp)
#define X	40 + STACK + ARGS(%esp)
#define INCX	44 + STACK + ARGS(%esp)
#define Y	48 + STACK + ARGS(%esp)
#define INCY	52 + STACK + ARGS(%esp)
#define BUFFER	56 + STACK + ARGS(%esp)


	PROLOGUE

	subl	$ARGS,	%esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	FLD	ALPHA

	movl	X, %edi			# X

	movl	$0, IS

	movl	M, %ebx
	movl	N, %eax

	testl	%ebx, %ebx
	jle	.L79
	testl	%eax, %eax
	jle	.L79

	movl	INCX, %esi
	sall	$BASE_SHIFT, %esi
	movl	%esi, INCX

	movl	INCY, %esi
	sall	$BASE_SHIFT, %esi
	movl	%esi, INCY

	movl	LDA,  %ebx

	imull	%ebx, %eax
	movl	$P,   %esi
	subl	%eax, %esi
	sall	$BASE_SHIFT, %esi
	movl	%esi, NLDA

	movl	%ebx, %esi
	sall	$BASE_SHIFT, %esi
	movl	%esi, LDA
	ALIGN_2

.L32:
	movl	IS,  %esi

	movl	$P, %edx
	movl	M,    %eax
	subl	%esi, %eax
	cmpl	%edx, %eax
#ifdef PENTIUM
	jle	.L33
	movl	%edx, %eax
.L33:
#else
	cmovg	%edx, %eax
#endif
	movl	%eax, MIN_M

	movl	IS, %ecx
	sall	$BASE_SHIFT, %ecx
	leal	(%edi,%ecx, 1), %ecx
	movl	INCX, %ebx
	movl	%ecx, XP
	cmpl	$SIZE, %ebx
	je	.L34

	movl	BUFFER, %esi
	movl	MIN_M, %ecx
	movl	%esi, XP
	sarl	$2, %ecx
	jle	.L35

	ALIGN_3

.L36:
	FLD	(%edi)
	addl	%ebx, %edi
	FST	0 * SIZE(%esi)

	FLD	(%edi)
	addl	%ebx, %edi
	FST	1 * SIZE(%esi)

	FLD	(%edi)
	addl	%ebx, %edi
	FST	2 * SIZE(%esi)

	FLD	(%edi)
	addl	%ebx, %edi
	FST	3 * SIZE(%esi)

	addl	$4 * SIZE, %esi
	decl	%ecx
	jg	.L36
	ALIGN_3

.L35:
	movl	MIN_M, %ecx
	andl	$3,%ecx
	jle	.L34
	ALIGN_2

.L42:
	FLD	(%edi)
	addl	%ebx,  %edi
	FST	(%esi)
	addl	$SIZE, %esi
	decl	%ecx
	jg	.L42
	ALIGN_3

/* Main Routine */

.L34:
	movl	Y, %ebp			# coffset = y

	movl	N, %esi
	sarl	$2, %esi
	movl	%esi, J
	jle	.L47
	ALIGN_3

.L48:
	movl	A, %ebx				# a_offset = a
	fldz
	movl	LDA, %edx
	fldz

	leal	(%ebx, %edx), %ecx		# a_offset2 = a + lda
	fldz
	leal	(%ebx, %edx, 4), %eax
	fldz

	movl	%eax, A
	movl	XP, %esi
	FLD	(%esi)

	movl	MIN_M, %eax
	sarl	$2,%eax
	jle	.L51
	ALIGN_3

#define PRESIZE 8

.L80:
#ifdef PENTIUM3
	prefetcht0	PRESIZE * SIZE(%ebx, %edx, 2)
	FLD	0 * SIZE(%ebx)		# at   = *(a_offset  + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1

	prefetcht0	PRESIZE * SIZE(%ecx)
	faddp	%st,%st(2)		# ct1 += at1
	FLD	0 * SIZE(%ecx)		# at1  = *(a_offset2 + 0 * lda)

	prefetcht0	PRESIZE * SIZE(%ecx, %edx, 2)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(3)		# ct2 += at1

	prefetcht0	PRESIZE * SIZE(%ebx)
	FLD	0 * SIZE(%ebx, %edx, 2)	# at   = *(a_offset  + 2 * lda)
	fmul	%st(1),%st

	faddp	%st,%st(4)
	FLD	0 * SIZE(%ecx, %edx, 2)	# at1  = *(a_offset2 + 2 * lda)
	fmulp	%st, %st(1)

	faddp	%st,%st(4)
	FLD	1 * SIZE(%esi)
	FLD	1 * SIZE(%ebx)		# at   = *(a_offset  + 0 * lda)

	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(2)		# ct1 += at1
	FLD	1 * SIZE(%ecx)		# at1  = *(a_offset2 + 0 * lda)

	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(3)		# ct2 += at1
	FLD	1 * SIZE(%ebx, %edx, 2)	# at   = *(a_offset  + 2 * lda)

	fmul	%st(1),%st
	faddp	%st,%st(4)
	FLD	1 * SIZE(%ecx, %edx, 2)	# at1  = *(a_offset2 + 2 * lda)

	fmulp	%st, %st(1)
	faddp	%st,%st(4)
	FLD	2 * SIZE(%esi)

	FLD	2 * SIZE(%ebx)		# at   = *(a_offset  + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(2)		# ct1 += at1

	FLD	2 * SIZE(%ecx)		# at1  = *(a_offset2 + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(3)		# ct2 += at1

	FLD	2 * SIZE(%ebx, %edx, 2)	# at   = *(a_offset  + 2 * lda)
	fmul	%st(1),%st
	faddp	%st,%st(4)

	FLD	2 * SIZE(%ecx, %edx, 2)	# at1  = *(a_offset2 + 2 * lda)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)

	FLD	3 * SIZE(%esi)
	FLD	3 * SIZE(%ebx)		# at   = *(a_offset  + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1

	faddp	%st,%st(2)		# ct1 += at1
	FLD	3 * SIZE(%ecx)		# at1  = *(a_offset2 + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1

	faddp	%st,%st(3)		# ct2 += at1
	FLD	3 * SIZE(%ebx, %edx, 2)	# at   = *(a_offset  + 2 * lda)
	fmul	%st(1),%st

	faddp	%st,%st(4)
	FLD	3 * SIZE(%ecx, %edx, 2)	# at1  = *(a_offset2 + 2 * lda)
	fmulp	%st, %st(1)

	addl	$4 * SIZE, %ebx
	faddp	%st,%st(4)
	addl	$4 * SIZE, %ecx

	FLD	4 * SIZE(%esi)
	addl	$4 * SIZE, %esi

#else

#if defined(HAS_PREFETCH)
       prefetcht0	PRESIZE * SIZE(%ebx)
       prefetcht0	PRESIZE * SIZE(%ebx, %edx, 2)
       prefetcht0	PRESIZE * SIZE(%ecx)
       prefetcht0	PRESIZE * SIZE(%ecx, %edx, 2)
#endif

	FLD	0 * SIZE(%ebx)		# at   = *(a_offset  + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(2)		# ct1 += at1

	FLD	0 * SIZE(%ecx)		# at1  = *(a_offset2 + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(3)		# ct2 += at1

	FLD	0 * SIZE(%ebx, %edx, 2)	# at   = *(a_offset  + 2 * lda)
	fmul	%st(1),%st
	faddp	%st,%st(4)

	FLD	0 * SIZE(%ecx, %edx, 2)	# at1  = *(a_offset2 + 2 * lda)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)
	FLD	1 * SIZE(%esi)

	FLD	1 * SIZE(%ebx)		# at   = *(a_offset  + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(2)		# ct1 += at1

	FLD	1 * SIZE(%ecx)		# at1  = *(a_offset2 + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(3)		# ct2 += at1

	FLD	1 * SIZE(%ebx, %edx, 2)	# at   = *(a_offset  + 2 * lda)
	fmul	%st(1),%st
	faddp	%st,%st(4)

	FLD	1 * SIZE(%ecx, %edx, 2)	# at1  = *(a_offset2 + 2 * lda)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)
	FLD	2 * SIZE(%esi)

	FLD	2 * SIZE(%ebx)		# at   = *(a_offset  + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(2)		# ct1 += at1

	FLD	2 * SIZE(%ecx)		# at1  = *(a_offset2 + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(3)		# ct2 += at1

	FLD	2 * SIZE(%ebx, %edx, 2)	# at   = *(a_offset  + 2 * lda)
	fmul	%st(1),%st
	faddp	%st,%st(4)

	FLD	2 * SIZE(%ecx, %edx, 2)	# at1  = *(a_offset2 + 2 * lda)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)
	FLD	3 * SIZE(%esi)

	FLD	3 * SIZE(%ebx)		# at   = *(a_offset  + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(2)		# ct1 += at1

	FLD	3 * SIZE(%ecx)		# at1  = *(a_offset2 + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(3)		# ct2 += at1

	FLD	3 * SIZE(%ebx, %edx, 2)	# at   = *(a_offset  + 2 * lda)
	fmul	%st(1),%st
	faddp	%st,%st(4)

	FLD	3 * SIZE(%ecx, %edx, 2)	# at1  = *(a_offset2 + 2 * lda)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)
	FLD	4 * SIZE(%esi)

	addl	$4 * SIZE, %ebx
	addl	$4 * SIZE, %ecx
	addl	$4 * SIZE, %esi
#endif

	decl	%eax
	jg	.L80
	ALIGN_3

.L51:
	movl	MIN_M, %eax
	andl	$3,    %eax
	je	.L81
	ALIGN_3

.L52:

	FLD	(%ebx)			# at   = *(a_offset  + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(2)		# ct1 += at1

	FLD	(%ecx)			# at1  = *(a_offset2 + 0 * lda)
	fmul	%st(1),%st		# at1 *= bt1
	faddp	%st,%st(3)		# ct2 += at1

	FLD	(%ebx, %edx, 2)		# at   = *(a_offset  + 2 * lda)
	fmul	%st(1),%st
	faddp	%st,%st(4)

	FLD	(%ecx, %edx, 2)		# at1  = *(a_offset2 + 2 * lda)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)
	FLD	1 * SIZE(%esi)

	addl	$SIZE, %ebx
	addl	$SIZE, %ecx
	addl	$SIZE, %esi
	decl	%eax
	jg	.L52
	ALIGN_3

.L81:
	ffreep	%st(0)

	fxch	%st(4)
	fmul	%st, %st(4)
	fmul	%st, %st(1)
	fmul	%st, %st(2)
	fmul	%st, %st(3)
	fxch	%st(4)

	movl	INCY, %eax

	FLD	(%ebp)
	faddp	%st, %st(1)
	FST	(%ebp)
	addl	%eax, %ebp

	FLD	(%ebp)
	faddp	%st, %st(1)
	FST	(%ebp)
	addl	%eax, %ebp

	FLD	(%ebp)
	faddp	%st, %st(1)
	FST	(%ebp)
	addl	%eax, %ebp

	FLD	(%ebp)
	faddp	%st, %st(1)
	FST	(%ebp)
	addl	%eax, %ebp

	decl	J
	jg	.L48
	ALIGN_3

.L47:
	movl	N, %esi
	andl	$3,%esi
	movl	%esi, J
	jle	.L60
	ALIGN_2

.L61:
	movl	A, %ebx			# a_offset = a
	fldz				# ct1 = ZERO
	movl	LDA, %edx
	fldz				# ct1 = ZERO

	addl	%ebx, %edx
	fldz				# ct1 = ZERO
	movl	%edx, A
	fldz				# ct1 = ZERO

	movl	XP, %esi

	movl	MIN_M, %eax
	sarl	$3,%eax
	jle	.L64
	ALIGN_3

.L65:
#ifdef HAS_PREFETCH
       prefetcht0	PRESIZE * 2 * SIZE(%ebx)
       prefetcht0	PRESIZE * 2 * SIZE(%ebx)
#endif

	FLD	0 * SIZE(%esi)
	FLD	0 * SIZE(%ebx)
	fmulp	%st, %st(1)
	faddp	%st,%st(1)

	FLD	1 * SIZE(%esi)
	FLD	1 * SIZE(%ebx)
	fmulp	%st, %st(1)
	faddp	%st,%st(2)

	FLD	2 * SIZE(%esi)
	FLD	2 * SIZE(%ebx)
	fmulp	%st, %st(1)
	faddp	%st,%st(3)

	FLD	3 * SIZE(%esi)
	FLD	3 * SIZE(%ebx)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)

	FLD	4 * SIZE(%esi)
	FLD	4 * SIZE(%ebx)
	fmulp	%st, %st(1)
	faddp	%st,%st(1)

	FLD	5 * SIZE(%esi)
	FLD	5 * SIZE(%ebx)
	fmulp	%st, %st(1)
	faddp	%st,%st(2)

	FLD	6 * SIZE(%esi)
	FLD	6 * SIZE(%ebx)
	fmulp	%st, %st(1)
	faddp	%st,%st(3)

	FLD	7 * SIZE(%esi)
	FLD	7 * SIZE(%ebx)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)

	addl	$8 * SIZE, %esi
	addl	$8 * SIZE, %ebx

	decl	%eax
	jg	.L65
	ALIGN_3

.L64:
	movl	MIN_M, %eax
	andl	$7, %eax
	jle	.L70
	ALIGN_3

.L71:
	FLD	(%esi)
	FLD	(%ebx)
	fmulp	%st, %st(1)
	faddp	%st,%st(1)

	addl	$SIZE, %esi
	addl	$SIZE, %ebx
	decl	%eax
	jg	.L71
	ALIGN_3

.L70:
	faddp	%st, %st(1)
	faddp	%st, %st(1)
	faddp	%st, %st(1)

	fmul	%st(1),%st
	FLD	(%ebp)
	faddp	%st, %st(1)
	FST	(%ebp)
	addl	INCY, %ebp
	decl	J
	jg	.L61
	ALIGN_3

.L60:
	movl	A, %ebx
	addl	NLDA, %ebx
	movl	%ebx, A

	addl	$P,  IS
	movl	M, %esi
	cmpl	%esi, IS
	jl	.L32
	ALIGN_3

.L79:
	ffreep	%st(0)
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
