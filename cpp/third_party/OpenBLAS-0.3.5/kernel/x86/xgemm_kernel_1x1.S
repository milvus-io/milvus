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

#if defined(OPTERON) || defined(BARCELONA)
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#else
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#endif

#define PREFETCHSIZE (5 + 4 * 10)
#define STACK	16
#define ARGS	16

#define J	 0 + STACK(%esp)
#define KK	 4 + STACK(%esp)
#define KKK	 8 + STACK(%esp)

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	32 + STACK + ARGS(%esp)
#define A	48 + STACK + ARGS(%esp)
#define ARG_B	52 + STACK + ARGS(%esp)
#define C	56 + STACK + ARGS(%esp)
#define ARG_LDC	60 + STACK + ARGS(%esp)
#define OFFSET	64 + STACK + ARGS(%esp)

#define I	%esi
#define B	%ebx
#define CO	%edi
#define AO	%edx
#define BO	%ecx
#define LDC	%ebp

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	  faddp
#define ADD2	  fsubrp
#define ADD3	  faddp
#define ADD4	  faddp
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	  faddp
#define ADD2	  faddp
#define ADD3	  fsubrp
#define ADD4	  faddp
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	  faddp
#define ADD2	  faddp
#define ADD3	  faddp
#define ADD4	  fsubrp
#else
#define ADD1	  faddp
#define ADD2	  fsubrp
#define ADD3	  fsubrp
#define ADD4	  fsubrp
#endif

#define PREFETCH_OFFSET 48

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

	movl	ARG_LDC, LDC
	movl	ARG_B,   B

	addl	$8 * SIZE, A
	addl	$8 * SIZE, B

	sall	$ZBASE_SHIFT, LDC

	cmpl	$0, M
	jle	.L999

	movl	N,   %eax
	movl	%eax, J
	testl	%eax, %eax
	jle	.L999
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	A, AO

	movl	C, CO
	addl	LDC, C

	movl	M,  I
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	B, BO
#else
	movl	KK,   %eax
	sall	$ZBASE_SHIFT, %eax
	leal	(AO, %eax, 1), AO
	leal	(B, %eax,  1), BO
#endif

	fldz
	fldz
	fldz
	fldz

#if   defined(HAVE_3DNOW)
	prefetchw	2 * SIZE(CO)
#elif defined(HAVE_SSE)
	prefetchnta	2 * SIZE(CO)
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
	sarl	$2, %eax
 	je	.L15
	ALIGN_4

.L12:
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(AO)

	FLD	 -8 * SIZE(AO)

	FLD	 -8 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	ADD1	 %st, %st(3)

	FLD	 -7 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -7 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

	FLD	 -6 * SIZE(AO)

	FLD	 -6 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	ADD1	 %st, %st(3)

	FLD	 -5 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -5 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

	PREFETCH	(PREFETCHSIZE + 4) * SIZE(AO)

	FLD	 -4 * SIZE(AO)

	FLD	 -4 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	ADD1	 %st, %st(3)

	FLD	 -3 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -3 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

	FLD	 -2 * SIZE(AO)

	FLD	 -2 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	ADD1	 %st, %st(3)

	FLD	 -1 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -1 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

	addl	$8 * SIZE,AO
	addl	$8 * SIZE,BO

	decl	%eax
	jne	.L12
	ALIGN_4

.L15:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	and	$3,  %eax
	je	.L18
	ALIGN_4

.L16:
	FLD	 -8 * SIZE(AO)

	FLD	 -8 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	ADD1	 %st, %st(3)

	FLD	 -7 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -7 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

	addl	$2 * SIZE,AO
	addl	$2 * SIZE,BO

	decl	%eax
	jne	 .L16
	ALIGN_4

.L18:
	faddp	%st, %st(3)
	faddp	%st, %st(1)

#ifndef TRMMKERNEL
	FLD	ALPHA_R
	fld	%st
	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	FLD	ALPHA_I
	fmul	%st, %st(3)
	fmulp	%st, %st(4)

	fsubp	%st, %st(2)
	faddp	%st, %st(2)

	FLD	0 * SIZE(CO)
	faddp	%st, %st(1)
	FST	0 * SIZE(CO)

	FLD	1 * SIZE(CO)
	faddp	%st, %st(1)
	FST	1 * SIZE(CO)
#else
	FST	1 * SIZE(CO)
	FST	0 * SIZE(CO)
#endif

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	sall	$ZBASE_SHIFT, %eax
	leal	(AO, %eax, 1), AO
	leal	(BO, %eax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, CO
	decl	I
	jne	.L11

#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$1, KK
#endif

	movl	BO, B
	decl	J
	jne	.L01
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
