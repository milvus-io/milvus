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

#define STACK	12
#define ARGS	 0

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#ifdef DOUBLE
#define BETA_R	16 + STACK + ARGS(%esp)
#define BETA_I	24 + STACK + ARGS(%esp)
#define C	48 + STACK + ARGS(%esp)
#define LDC	52 + STACK + ARGS(%esp)
#else
#define BETA_R	16 + STACK + ARGS(%esp)
#define BETA_I	20 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define LDC	44 + STACK + ARGS(%esp)
#endif

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi

	PROFCODE

	movl	M,   %ebp
	movl	N,   %ecx
	movl	LDC, %edx
	movl	C,   %edi

	FLD	BETA_R
	FLD	BETA_I

	testl	%ebp, %ebp		# if n <= 0 goto End
	jle	.L83
	testl	%ecx, %ecx		# if m <= 0 goto End
	jle	.L83

	fld	%st(1)
	fabs
	fld	%st(1)
	fabs
	faddp	%st, %st(1)

	sall	$ZBASE_SHIFT,  %edx

	ftst
	fnstsw	%ax
	andb	$68, %ah
#ifndef C_SUN
	ffreep	%st(0)
#else
	.byte	0xdf
	.byte	0xc0
#endif
	je	.L71
	ALIGN_2

.L53:
	movl	%edi, %esi		# c_offset1 = c_offset
	addl	%edx, %edi		# c_offset += ldc

	movl	%ebp, %eax
	sarl	$2,   %eax
	jle	.L56
	ALIGN_2

.L57:
#if defined(HAS_PREFETCH) && defined(PENTIUM3)
	prefetchnta	16 * SIZE(%esi)
	prefetchnta	24 * SIZE(%esi)
#endif

	FSTU	0 * SIZE(%esi)		# c_offset1
	FSTU	1 * SIZE(%esi)
	FSTU	2 * SIZE(%esi)
	FSTU	3 * SIZE(%esi)
	FSTU	4 * SIZE(%esi)
	FSTU	5 * SIZE(%esi)
	FSTU	6 * SIZE(%esi)
	FSTU	7 * SIZE(%esi)
	addl	$8 * SIZE, %esi		# c_offset1 += 8
	decl	%eax			# i--
	jg	.L57
	ALIGN_2

.L56:
	movl	%ebp, %eax
	andl	$3,   %eax
	jle	.L62
	ALIGN_2

.L63:
	FSTU	0 * SIZE(%esi)
	FSTU	1 * SIZE(%esi)
	addl	$2 * SIZE,%esi
	decl	%eax
	jg	.L63
	ALIGN_2

.L62:
	decl	%ecx			# j --
	jg	.L53
	jmp	.L83
	ALIGN_3

.L71:
	movl	%edi, %esi
	addl	%edx, %edi		# c_offset += ldc


	movl	%ebp, %eax
	sarl	$1,   %eax
	jle	.L84
	ALIGN_3

.L85:
#if defined(HAS_PREFETCH) && defined(PENTIUM3)
	prefetchnta	16 * SIZE(%esi)
#endif
	fld	%st(0)
	FMUL	0 * SIZE(%esi)
	fld	%st(2)
	FMUL	1 * SIZE(%esi)
	faddp	%st,%st(1)
	fld	%st(2)
	FMUL	0 * SIZE(%esi)
	fld	%st(2)
	FMUL	1 * SIZE(%esi)
	fsubrp	%st,%st(1)

	FST	0 * SIZE(%esi)
	FST	1 * SIZE(%esi)

	fld	%st(0)
	FMUL	2 * SIZE(%esi)
	fld	%st(2)
	FMUL	3 * SIZE(%esi)
	faddp	%st,%st(1)
	fld	%st(2)
	FMUL	2 * SIZE(%esi)
	fld	%st(2)
	FMUL	3 * SIZE(%esi)
	fsubrp	%st,%st(1)

	FST	2 * SIZE(%esi)
	FST	3 * SIZE(%esi)
	addl	$4 * SIZE, %esi

	decl	%eax
	jg	.L85
	ALIGN_3

.L84:
	movl	%ebp, %eax
	andl	$1,   %eax
	jle	.L74
	ALIGN_3

.L75:
#if defined(HAS_PREFETCH) && defined(PENTIUM3)
	prefetchnta	16 * SIZE(%esi)
#endif

	fld	%st(0)
	FMUL	0 * SIZE(%esi)
	fld	%st(2)
	FMUL	1 * SIZE(%esi)
	faddp	%st,%st(1)
	fld	%st(2)
	FMUL	0 * SIZE(%esi)
	fld	%st(2)
	FMUL	1 * SIZE(%esi)
	fsubrp	%st,%st(1)

	FST	0 * SIZE(%esi)
	FST	1 * SIZE(%esi)
	ALIGN_2

.L74:
	decl	%ecx
	jg	.L71
	ALIGN_2

.L83:
#ifndef C_SUN
	ffreep	%st(0)
	ffreep	%st(0)
#else
	.byte	0xdf
	.byte	0xc0
	.byte	0xdf
	.byte	0xc0
#endif

	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
