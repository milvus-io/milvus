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
#define ARGS	 0

#define STACK_M		 4 + STACK + ARGS(%esp)
#define STACK_X		 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)

#define	M	%ebx
#define X	%ecx
#define INCX	%edx
#define I	%eax

#ifdef USE_MIN
#define maxpd	minpd
#define maxsd	minsd
#endif

#include "l1param.h"

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

	movl	STACK_M, M
	movl	STACK_X, X
	movl	STACK_INCX, INCX

	xorps	%xmm0, %xmm0
	leal	(, INCX, SIZE), INCX

	testl	M, M
	jle	.L999

#ifdef USE_ABS
	pcmpeqb	%xmm3, %xmm3
	psrlq	$1, %xmm3
#endif

	movsd	(X), %xmm0
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm0
#endif
	unpcklpd %xmm0, %xmm0
	movaps	 %xmm0, %xmm1
	decl	M
	jle	.L999

	cmpl	$SIZE, INCX
	jne	.L40

	subl	$-16 * SIZE, X

	testl	$SIZE, X
	je	.L05

	movsd	-16 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	unpcklpd  %xmm4, %xmm4
	maxpd	  %xmm4, %xmm0
	addl	$SIZE, X
	decl	M
	jle	.L998
	ALIGN_3

.L05:
	movl	M,  I
	sarl	$4, I
	jle	.L15

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7

	decl	I
	jle	.L12
	ALIGN_4

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	movaps	 -6 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	 %xmm3, %xmm6
#endif
	maxpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	 %xmm3, %xmm7
#endif
	maxpd	%xmm7, %xmm1
	movaps	 -2 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	movaps	  2 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	 %xmm3, %xmm6
#endif
	maxpd	%xmm6, %xmm0
	movaps	  4 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	 %xmm3, %xmm7
#endif
	maxpd	%xmm7, %xmm1
	movaps	  6 * SIZE(X), %xmm7

	subl	$-16 * SIZE, X
	decl	I
	jg	.L11
	ALIGN_4

.L12:
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	movaps	 -6 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	 %xmm3, %xmm6
#endif
	maxpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	 %xmm3, %xmm7
#endif
	maxpd	%xmm7, %xmm1
	movaps	 -2 * SIZE(X), %xmm7

#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0

#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1

#ifdef USE_ABS
	andps	 %xmm3, %xmm6
#endif
	maxpd	%xmm6, %xmm0

#ifdef USE_ABS
	andps	 %xmm3, %xmm7
#endif
	maxpd	%xmm7, %xmm1

	subl	$-16 * SIZE, X
	ALIGN_4

.L15:
	testl	$8, M
	jle	.L16

	movaps	-16 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movaps	-14 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movaps	-12 * SIZE(X), %xmm6
#ifdef USE_ABS
	andps	 %xmm3, %xmm6
#endif
	maxpd	%xmm6, %xmm0

	movaps	-10 * SIZE(X), %xmm7
#ifdef USE_ABS
	andps	 %xmm3, %xmm7
#endif
	maxpd	%xmm7, %xmm1
	addl	$8 * SIZE, X
	ALIGN_3

.L16:
	testl	$4, M
	jle	.L17

	movaps	-16 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movaps	-14 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	addl	$4 * SIZE, X
	ALIGN_3

.L17:
	testl	$2, M
	jle	.L18

	movaps	-16 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0
	addl	$2 * SIZE, X
	ALIGN_3

.L18:
	testl	$1, M
	jle	.L998

	movsd	-16 * SIZE(X), %xmm4
	unpcklpd  %xmm4, %xmm4
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm1
	jmp	.L998
	ALIGN_3

.L40:
	movl	M,  I
	sarl	$4, I
	jle	.L45
	ALIGN_4

.L41:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	(X), %xmm6
	addl	INCX, X
	movhps	(X), %xmm6
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm6
#endif
	maxpd	%xmm6, %xmm0

	movsd	(X), %xmm7
	addl	INCX, X
	movhps	(X), %xmm7
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm7
#endif
	maxpd	%xmm7, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	(X), %xmm6
	addl	INCX, X
	movhps	(X), %xmm6
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm6
#endif
	maxpd	%xmm6, %xmm0

	movsd	(X), %xmm7
	addl	INCX, X
	movhps	(X), %xmm7
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm7
#endif
	maxpd	%xmm7, %xmm1

	decl	I
	jg	.L41
	ALIGN_4

.L45:
	andl	$15,  M
	jle	.L998

	testl	$8, M
	je	.L46

	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	(X), %xmm6
	addl	INCX, X
	movhps	(X), %xmm6
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm6
#endif
	maxpd	%xmm6, %xmm0

	movsd	(X), %xmm7
	addl	INCX, X
	movhps	(X), %xmm7
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm7
#endif
	maxpd	%xmm7, %xmm1
	ALIGN_3

.L46:
	testl	$4, M
	je	.L47

	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	ALIGN_3

.L47:
	testl	$2, M
	je	.L48

	movsd	(X), %xmm6
	addl	INCX, X
	movhps	(X), %xmm6
	addl	INCX, X
#ifdef USE_ABS
	andps	 %xmm3, %xmm6
#endif
	maxpd	%xmm6, %xmm0
	ALIGN_3

.L48:
	testl	$1, M
	je	.L998

	movsd	(X), %xmm7
	unpcklpd  %xmm7, %xmm7
#ifdef USE_ABS
	andps	 %xmm3, %xmm7
#endif
	maxpd	%xmm7, %xmm1
	ALIGN_4

.L998:
	maxpd	%xmm1, %xmm0
	movaps	%xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	%xmm1, %xmm0
	ALIGN_4

.L999:
	subl	$8, %esp
	movsd	%xmm0, (%esp)
	fldl	(%esp)
	addl	$8, %esp
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
