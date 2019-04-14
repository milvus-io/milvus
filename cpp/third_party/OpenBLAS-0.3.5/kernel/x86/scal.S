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

	PROLOGUE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

	movl	16(%esp),%edx
	FLD	28(%esp)

#ifdef XDOUBLE
	movl	44(%esp),%edi
	movl	48(%esp),%esi
#elif defined(DOUBLE)
	movl	36(%esp),%edi
	movl	40(%esp),%esi
#else
	movl	32(%esp),%edi
	movl	36(%esp),%esi
#endif

	ftst
	fnstsw	%ax
	andb	$68, %ah
	je	.L300		# Alpha != ZERO

/* Alpha == ZERO */
	cmpl	$1,%esi
	jne	.L104

	movl	%edx, %ecx		# ecx = n
	sarl	$3,   %ecx		# (n >> 3)
	jle	.L102
	ALIGN_4

.L101:
#ifndef XDOUBLE
	FSTU	0 * SIZE(%edi)
	FSTU	1 * SIZE(%edi)
	FSTU	2 * SIZE(%edi)
	FSTU	3 * SIZE(%edi)
	FSTU	4 * SIZE(%edi)
	FSTU	5 * SIZE(%edi)
	FSTU	6 * SIZE(%edi)
	FSTU	7 * SIZE(%edi)
#else
	fld	%st
	FST	0 * SIZE(%edi)
	fld	%st
	FST	1 * SIZE(%edi)
	fld	%st
	FST	2 * SIZE(%edi)
	fld	%st
	FST	3 * SIZE(%edi)
	fld	%st
	FST	4 * SIZE(%edi)
	fld	%st
	FST	5 * SIZE(%edi)
	fld	%st
	FST	6 * SIZE(%edi)
	fld	%st
	FST	7 * SIZE(%edi)
#endif

	addl	$8 * SIZE, %edi
	decl	%ecx
	jg	.L101
	ALIGN_4

.L102:
	movl	%edx, %ecx
	andl	$7,   %ecx
	jle	.L999
	ALIGN_4

.L103:
#ifndef XDOUBLE
	FSTU	0 * SIZE(%edi)
#else
	fld	%st
	FST	0 * SIZE(%edi)
#endif
	addl	$SIZE, %edi
	decl	%ecx
	jg	.L103
	jmp	.L999
	ALIGN_4

.L104:
	sall	$BASE_SHIFT, %esi

	movl	%edx, %ecx		# ecx = n
	sarl	$3,   %ecx		# (n >> 3)
	jle	.L106
	ALIGN_4

.L105:
#ifndef XDOUBLE
	FSTU	0 * SIZE(%edi)
	addl	%esi, %edi
	FSTU	0 * SIZE(%edi)
	addl	%esi, %edi
	FSTU	0 * SIZE(%edi)
	addl	%esi, %edi
	FSTU	0 * SIZE(%edi)
	addl	%esi, %edi
	FSTU	0 * SIZE(%edi)
	addl	%esi, %edi
	FSTU	0 * SIZE(%edi)
	addl	%esi, %edi
	FSTU	0 * SIZE(%edi)
	addl	%esi, %edi
	FSTU	0 * SIZE(%edi)
	addl	%esi, %edi
#else
	fld	%st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi
	fld	%st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi
	fld	%st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi
	fld	%st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi
	fld	%st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi
	fld	%st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi
	fld	%st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi
	fld	%st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi
#endif

	decl	%ecx
	jg	.L105
	ALIGN_4

.L106:
	movl	%edx, %ecx
	andl	$7,   %ecx
	jle	.L999
	ALIGN_4

.L107:
#ifndef XDOUBLE
	FSTU	0 * SIZE(%edi)
#else
	fld	%st
	FST	0 * SIZE(%edi)
#endif
	addl	%esi, %edi
	decl	%ecx
	jg	.L107
	jmp	.L999
	ALIGN_4

/* Alpha != ZERO */

.L300:
	cmpl	$1,%esi
	jne	.L304

	movl	%edx, %ecx		# ecx = n
	sarl	$3,   %ecx		# (n >> 3)
	jle	.L302
	ALIGN_4

.L301:
	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)

	FLD	1 * SIZE(%edi)
	fmul	%st(1), %st
	FST	1 * SIZE(%edi)

	FLD	2 * SIZE(%edi)
	fmul	%st(1), %st
	FST	2 * SIZE(%edi)

	FLD	3 * SIZE(%edi)
	fmul	%st(1), %st
	FST	3 * SIZE(%edi)

	FLD	4 * SIZE(%edi)
	fmul	%st(1), %st
	FST	4 * SIZE(%edi)

	FLD	5 * SIZE(%edi)
	fmul	%st(1), %st
	FST	5 * SIZE(%edi)

	FLD	6 * SIZE(%edi)
	fmul	%st(1), %st
	FST	6 * SIZE(%edi)

	FLD	7 * SIZE(%edi)
	fmul	%st(1), %st
	FST	7 * SIZE(%edi)

	addl	$8 * SIZE, %edi
	decl	%ecx
	jg	.L301
	ALIGN_4

.L302:
	movl	%edx, %ecx
	andl	$7,   %ecx
	jle	.L999
	ALIGN_4

.L303:
	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	$SIZE, %edi
	decl	%ecx
	jg	.L303
	jmp	.L999
	ALIGN_4

.L304:
	sall	$BASE_SHIFT, %esi

	movl	%edx, %ecx		# ecx = n
	sarl	$3,   %ecx		# (n >> 3)
	jle	.L306
	ALIGN_4

.L305:
	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi

	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi

	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi

	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi

	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi

	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi

	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi

	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi

	decl	%ecx
	jg	.L305
	ALIGN_4

.L306:
	movl	%edx, %ecx
	andl	$7,   %ecx
	jle	.L999
	ALIGN_4

.L307:
	FLD	0 * SIZE(%edi)
	fmul	%st(1), %st
	FST	0 * SIZE(%edi)
	addl	%esi, %edi
	decl	%ecx
	jg	.L307
	ALIGN_4

.L999:
	ffreep	%st(0)
	xorl	%eax,%eax
	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
