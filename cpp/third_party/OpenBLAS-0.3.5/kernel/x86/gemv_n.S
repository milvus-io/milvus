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

#if defined(ATHLON) || defined(OPTERON) || defined(OPTERON)
#define P 32
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
#define ALPHA	16 + STACK + ARGS(%esp)
#ifdef DOUBLE
#define A	24 + STACK + ARGS(%esp)
#define LDA	28 + STACK + ARGS(%esp)
#define X	32 + STACK + ARGS(%esp)
#define INCX	36 + STACK + ARGS(%esp)
#define Y	40 + STACK + ARGS(%esp)
#define INCY	44 + STACK + ARGS(%esp)
#define BUFFER	48 + STACK + ARGS(%esp)
#else
#define A	20 + STACK + ARGS(%esp)
#define LDA	24 + STACK + ARGS(%esp)
#define X	28 + STACK + ARGS(%esp)
#define INCX	32 + STACK + ARGS(%esp)
#define Y	36 + STACK + ARGS(%esp)
#define INCY	40 + STACK + ARGS(%esp)
#define BUFFER	44 + STACK + ARGS(%esp)
#endif

	PROLOGUE

	subl	$ARGS, %esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	FLD	ALPHA
	movl	X, %edi

	movl	LDA, %ebx
	leal	0(,%ebx,SIZE),%ebx	# EBX : lda

	movl	$0, IS
	movl	M, %edx
	movl	N, %esi

	test	%esi, %esi
	jle	.L79			# goto END
	test	%edx, %edx
	jle	.L79			# goto END

	movl	INCY, %eax
	leal	(,%eax,SIZE),%eax
	movl	%eax, INCY

	movl	LDA, %eax
	imull	$P,  %eax		# P * lda
	subl	M   ,%eax		# P * lda - m
	leal	(, %eax, SIZE), %eax
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
	movl	INCX, %edx

	leal	(%edi, %esi, SIZE), %esi # xp = x + is
	movl	%esi, XP
	cmpl	$1, %edx
	je	.L34			# if incx == 1 goto L34

	movl	BUFFER, %esi
	leal	(, %edx, SIZE), %edx
	movl	%esi, XP		# xp = buffer
	sarl	$2,%eax
	jle	.L35
	ALIGN_2

.L36:
	FLD	(%edi)
	addl	%edx,%edi		# x += incx
	FLD	(%edi)
	addl	%edx,%edi		# x += incx
	FLD	(%edi)
	addl	%edx,%edi		# x += incx
	FLD	(%edi)
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
	andl	$3,    %eax
	jle	.L34
	ALIGN_2

.L42:
	FLD	(%edi)
	addl	%edx,  %edi
	FST	(%esi)
	addl	$SIZE, %esi
	decl	%eax
	jg	.L42
	ALIGN_3

/* Main Routine */
.L34:
	movl	 Y, %ecx		# c_offset
	movl	 M, %ebp
	sarl	$2, %ebp		# j = (m >> 2)
	jle	.L47
	ALIGN_2

.L48:
	movl	A, %edx			# a_offset = a
	fldz
	addl	$4 * SIZE, A		# a += 4
	fldz
	movl	XP, %esi		# b_offset = xp
	fldz
	movl	MIN_N, %eax		# i = min_n
	fldz
	FLD	(%esi)			# bt1 = b_offset
	sarl	$1,    %eax
	jle	.L51
	ALIGN_2

#ifdef PENTIUM3
#define PRESIZE  8
#else
#define PRESIZE 24
#endif

.L80:
#ifdef PENTIUM3
       prefetcht1	PRESIZE * SIZE(%edx, %ebx, 1)
	FLD	0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1), %st		# at1 *= bt1

       prefetcht1	PRESIZE * SIZE(%esi)
	faddp	%st, %st(2)		# ct1 += at1
	FLD	1 * SIZE(%edx)		# at1  = *(a_offset + 1)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(3)		# ct2 += at1
	FLD	2 * SIZE(%edx)		# at1  = *(a_offset + 2)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1
	FLD	3 * SIZE(%edx)		# bt1 *= *(a_offset + 3)

	fmulp	%st, %st(1)
	faddp	%st, %st(4)		# ct4 += at1
	FLD	1 * SIZE(%esi)		# bt1 = b_offset

       prefetcht1	PRESIZE * SIZE(%edx, %ebx, 2)
	addl	%ebx, %edx		# a_offset += lda
	FLD	0 * SIZE(%edx)		# at1  = *(a_offset + 0)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1
	FLD	1 * SIZE(%edx)		# at1  = *(a_offset + 1)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(3)		# ct2 += at1
	FLD	2 * SIZE(%edx)		# at1  = *(a_offset + 2)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1
	FLD	3 * SIZE(%edx)		# bt1 *= *(a_offset + 3)

	fmulp	%st, %st(1)
	addl	%ebx, %edx
	faddp	%st, %st(4)		# ct4 += at1

	FLD	2 * SIZE(%esi)		# bt1 = b_offset
	addl	$2 * SIZE, %esi		# b_offset += 2

#else
#ifdef PENTIUM4
       prefetchnta	 8 * SIZE(%esi)
#endif
	FLD	0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	1 * SIZE(%edx)		# at1  = *(a_offset + 1)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(3)		# ct2 += at1

	FLD	2 * SIZE(%edx)		# at1  = *(a_offset + 2)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FMUL	3 * SIZE(%edx)		# bt1 *= *(a_offset + 3)
	faddp	%st, %st(4)		# ct4 += at1
	FLD	1 * SIZE(%esi)		# bt1 = b_offset

	addl	%ebx, %edx		# a_offset += lda

	FLD	0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	1 * SIZE(%edx)		# at1  = *(a_offset + 1)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(3)		# ct2 += at1

	FLD	2 * SIZE(%edx)		# at1  = *(a_offset + 2)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FMUL	3 * SIZE(%edx)		# bt1 *= *(a_offset + 3)
	faddp	%st, %st(4)		# ct4 += at1
	FLD	2 * SIZE(%esi)		# bt1 = b_offset

	addl	%ebx, %edx
	addl	$2 * SIZE, %esi		# b_offset += 2
#endif
	decl	%eax
	jg	.L80

.L51:
	movl	MIN_N,%eax
	andl	$1, %eax
	je	.L57

	FLD	0 * SIZE(%edx)		# at1  = *(a_offset + 0)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	1 * SIZE(%edx)		# at1  = *(a_offset + 1)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(3)		# ct2 += at1

	FLD	2 * SIZE(%edx)		# at1  = *(a_offset + 2)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FMUL	3 * SIZE(%edx)		# bt1 *= *(a_offset + 3)
	faddp	%st, %st(4)		# ct4 += at1
	fldz
	ALIGN_2

.L57:
#ifndef C_SUN
	ffreep	%st(0)
#else
	.byte	0xdf
	.byte	0xc0
#endif

	fxch	%st(4)
	fmul	%st, %st(4)
	fmul	%st, %st(1)
	fmul	%st, %st(2)
	fmul	%st, %st(3)
	fxch	%st(4)

	movl	INCY, %eax

	FADD	(%ecx)
	FST	(%ecx)
	addl	%eax, %ecx

	FADD	(%ecx)
	FST	(%ecx)
	addl	%eax, %ecx

	FADD	(%ecx)
	FST	(%ecx)
	addl	%eax, %ecx

	FADD	(%ecx)
	FST	(%ecx)
	addl	%eax, %ecx

	decl	%ebp		# j --
	jg	.L48
	ALIGN_3

.L47:
	movl	M,  %ebp
	andl	$3, %ebp		# j = (m & 3)
	jle	.L60
	ALIGN_2

.L61:

	movl	A, %edx			# a_offset = a
	fldz
	addl	$SIZE, A		# a++
	fldz
	movl	XP,%esi
	fldz
	movl	MIN_N,%eax
	fldz
	sarl	$3,%eax
	jle	.L64
	ALIGN_2

.L65:
	FLD	0 * SIZE(%esi)
	FMUL	(%edx)
	faddp	%st, %st(1)
	addl	%ebx, %edx

	FLD	1 * SIZE(%esi)
	FMUL	(%edx)
	faddp	%st, %st(2)
	addl	%ebx ,%edx

	FLD	2 * SIZE(%esi)
	FMUL	(%edx)
	faddp	%st, %st(3)
	addl	%ebx, %edx

	FLD	3 * SIZE(%esi)
	FMUL	(%edx)
	faddp	%st, %st(4)
	addl	%ebx, %edx

	FLD	4 * SIZE(%esi)
	FMUL	(%edx)
	faddp	%st,%st(1)
	addl	%ebx, %edx

	FLD	5 * SIZE(%esi)
	FMUL	(%edx)
	faddp	%st, %st(2)
	addl	%ebx, %edx

	FLD	6 * SIZE(%esi)
	FMUL	(%edx)
	faddp	%st,%st(3)
	addl	%ebx, %edx

	FLD	7 * SIZE(%esi)
	FMUL	(%edx)
	faddp	%st,%st(4)
	addl	%ebx, %edx

	addl	$8 * SIZE, %esi
	decl	%eax
	jg	.L65

.L64:
	movl	MIN_N,%eax
	andl	$7, %eax
	jle	.L70
	ALIGN_2

.L71:
	FLD	(%esi)
	addl	$SIZE, %esi	 # b_offset ++
	FMUL	(%edx)
	addl	%ebx,  %edx	 # a_offset += lda
	faddp	%st, %st(1)
	decl	%eax
	jg	.L71
	ALIGN_2

.L70:
	faddp	%st, %st(1)
	faddp	%st, %st(1)
	faddp	%st, %st(1)

	fmul	%st(1), %st
	movl	INCY,  %eax
	FADD	(%ecx)
	FST	(%ecx)
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
#ifndef C_SUN
	ffreep	%st(0)
#else
	.byte	0xdf
	.byte	0xc0
#endif

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
