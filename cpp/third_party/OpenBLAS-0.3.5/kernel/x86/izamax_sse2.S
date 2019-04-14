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

#define RET	%eax
#define	M	%ebx
#define X	%ecx
#define INCX	%edx
#define I	%esi
#define MM	%ebp
#define XX	%edi
#define TEMP	%ebx

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

	movl	STACK_M, M
	movl	STACK_X, X
	movl	STACK_INCX, INCX

#ifdef F_INTERFACE
	movl	(M), M
	movl	(INCX), INCX
#endif

	pxor	%xmm0, %xmm0
	pxor	%xmm7, %xmm7
	xor	RET, RET
	testl	M, M
	jle	.L999
	testl	INCX, INCX
	jle	.L999

	sall	$ZBASE_SHIFT, INCX
	movl	M, MM
	movl	X, XX

	cmpeqpd	%xmm7, %xmm7
	psrlq	$1, %xmm7

	movsd	0 * SIZE(XX), %xmm0
	movsd	1 * SIZE(XX), %xmm1
	addl	INCX, XX
	decl	MM
	andpd	 %xmm7, %xmm0
	andpd	 %xmm7, %xmm1
	addpd	 %xmm1, %xmm0
	unpcklpd %xmm0, %xmm0
	cmpl	$2 * SIZE, INCX
	jne	.L60

	movl	MM,  I
	sarl	$3, I
	jle	.L25
	ALIGN_4

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movsd	 0 * SIZE(XX), %xmm1
	movsd	 1 * SIZE(XX), %xmm2
	movhpd	 2 * SIZE(XX), %xmm1
	movhpd	 3 * SIZE(XX), %xmm2

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxpd	%xmm1, %xmm0

	movsd	 4 * SIZE(XX), %xmm3
	movsd	 5 * SIZE(XX), %xmm4
	movhpd	 6 * SIZE(XX), %xmm3
	movhpd	 7 * SIZE(XX), %xmm4

	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4
	addpd	%xmm4, %xmm3
	maxpd	%xmm3, %xmm0

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(XX)
#endif

	movsd	 8 * SIZE(XX), %xmm1
	movsd	 9 * SIZE(XX), %xmm2
	movhpd	10 * SIZE(XX), %xmm1
	movhpd	11 * SIZE(XX), %xmm2

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxpd	%xmm1, %xmm0

	movsd	12 * SIZE(XX), %xmm3
	movsd	13 * SIZE(XX), %xmm4
	movhpd	14 * SIZE(XX), %xmm3
	movhpd	15 * SIZE(XX), %xmm4

	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4
	addpd	%xmm4, %xmm3
	maxpd	%xmm3, %xmm0

	addl	$16 * SIZE, XX
	decl	I
	jg	.L21
	ALIGN_4

.L25:
	andl	$7,  MM
	jle	.L30

	testl	$4, MM
	je	.L26

	movsd	 0 * SIZE(XX), %xmm1
	movsd	 1 * SIZE(XX), %xmm2
	movhpd	 2 * SIZE(XX), %xmm1
	movhpd	 3 * SIZE(XX), %xmm2

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxpd	%xmm1, %xmm0

	movsd	 4 * SIZE(XX), %xmm3
	movsd	 5 * SIZE(XX), %xmm4
	movhpd	 6 * SIZE(XX), %xmm3
	movhpd	 7 * SIZE(XX), %xmm4

	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4
	addpd	%xmm4, %xmm3
	maxpd	%xmm3, %xmm0
	addl	$8 * SIZE, XX
	ALIGN_3

.L26:
	testl	$2, MM
	je	.L27

	movsd	 0 * SIZE(XX), %xmm1
	movsd	 1 * SIZE(XX), %xmm2
	movhpd	 2 * SIZE(XX), %xmm1
	movhpd	 3 * SIZE(XX), %xmm2

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxpd	%xmm1, %xmm0

	addl	$4 * SIZE, XX
	ALIGN_3

.L27:
	testl	$1, MM
	je	.L30

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxsd	%xmm1, %xmm0
	ALIGN_4

.L30:
	movl	X, XX
	movl	M, MM

	movapd	 %xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	 %xmm1, %xmm0
	unpcklpd %xmm0, %xmm0

	movl	MM,  I
	sarl	$2, I
	jle	.L35
	ALIGN_4

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	movhpd	2 * SIZE(XX), %xmm1
	movhpd	3 * SIZE(XX), %xmm2
	movsd	4 * SIZE(XX), %xmm3
	movsd	5 * SIZE(XX), %xmm4
	movhpd	6 * SIZE(XX), %xmm3
	movhpd	7 * SIZE(XX), %xmm4

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4

	addpd	%xmm2,  %xmm1
	addpd	%xmm4,  %xmm3

	cmpeqpd	%xmm0, %xmm1
	cmpeqpd	%xmm0, %xmm3

	orpd	%xmm3, %xmm1
	movmskpd %xmm1, TEMP
	testl	 $3, TEMP
	jne	 .L33

	addl	$8 * SIZE, XX
	addl	$4, RET
	decl	I
	jg	.L31
	jmp	.L35
	ALIGN_4

.L33:
	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	movsd	2 * SIZE(XX), %xmm3
	movsd	3 * SIZE(XX), %xmm4

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4

	addpd	%xmm2,  %xmm1
	addpd	%xmm4,  %xmm3

	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999

	movsd	4 * SIZE(XX), %xmm1
	movsd	5 * SIZE(XX), %xmm2
	movsd	6 * SIZE(XX), %xmm3
	movsd	7 * SIZE(XX), %xmm4
	addl	$8 * SIZE, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4

	addpd	%xmm2,  %xmm1
	addpd	%xmm4,  %xmm3

	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	ALIGN_3

.L35:
	testl	$2, MM
	je	.L36

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	movsd	2 * SIZE(XX), %xmm3
	movsd	3 * SIZE(XX), %xmm4
	addl	$4 * SIZE, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4

	addpd	%xmm2,  %xmm1
	addpd	%xmm4,  %xmm3

	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	ALIGN_3

.L36:
	incl	RET
	jmp	.L999
	ALIGN_3

.L60:
	movl	MM,  I
	sarl	$3, I
	jle	.L65
	ALIGN_4

.L61:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm1
	movhpd	1 * SIZE(XX), %xmm2
	addl	INCX, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxpd	%xmm1, %xmm0

	movsd	0 * SIZE(XX), %xmm3
	movsd	1 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm3
	movhpd	1 * SIZE(XX), %xmm4
	addl	INCX, XX

	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4
	addpd	%xmm4, %xmm3
	maxpd	%xmm3, %xmm0

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm1
	movhpd	1 * SIZE(XX), %xmm2
	addl	INCX, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxpd	%xmm1, %xmm0

	movsd	0 * SIZE(XX), %xmm3
	movsd	1 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm3
	movhpd	1 * SIZE(XX), %xmm4
	addl	INCX, XX

	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4
	addpd	%xmm4, %xmm3
	maxpd	%xmm3, %xmm0

	decl	I
	jg	.L61
	ALIGN_4

.L65:
	andl	$7,  MM
	jle	.L70

	testl	$4, MM
	je	.L66

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm1
	movhpd	1 * SIZE(XX), %xmm2
	addl	INCX, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxpd	%xmm1, %xmm0

	movsd	0 * SIZE(XX), %xmm3
	movsd	1 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm3
	movhpd	1 * SIZE(XX), %xmm4
	addl	INCX, XX

	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4
	addpd	%xmm4, %xmm3
	maxpd	%xmm3, %xmm0
	ALIGN_3

.L66:
	testl	$2, MM
	je	.L67

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm1
	movhpd	1 * SIZE(XX), %xmm2
	addl	INCX, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxpd	%xmm1, %xmm0
	ALIGN_3

.L67:
	testl	$1, MM
	je	.L70

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	addpd	%xmm2, %xmm1
	maxsd	%xmm1, %xmm0
	ALIGN_3

.L70:
	movl	X, XX
	movl	M, MM

	movapd	%xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	%xmm1, %xmm0
	unpcklpd %xmm0, %xmm0

	movl	MM,  I
	sarl	$2, I
	jle	.L75
	ALIGN_4

.L71:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm1
	movhpd	1 * SIZE(XX), %xmm2
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm3
	movsd	1 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm3
	movhpd	1 * SIZE(XX), %xmm4
	addl	INCX, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4

	addpd	%xmm2,  %xmm1
	addpd	%xmm4,  %xmm3

	cmpeqpd	%xmm0, %xmm1
	cmpeqpd	%xmm0, %xmm3

	orpd	%xmm3, %xmm1
	movmskpd %xmm1, TEMP
	testl	 $3, TEMP
	jne	 .L73

	addl	$4, RET
	decl	I
	jg	.L71
	jmp	.L75
	ALIGN_4

.L73:
	leal	(, INCX, 4), TEMP
	subl	TEMP, XX

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm3
	movsd	1 * SIZE(XX), %xmm4
	addl	INCX, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4

	addpd	%xmm2,  %xmm1
	addpd	%xmm4,  %xmm3

	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm3
	movsd	1 * SIZE(XX), %xmm4
	addl	INCX, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4

	addpd	%xmm2,  %xmm1
	addpd	%xmm4,  %xmm3

	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	ALIGN_3

.L75:
	testl	$2, MM
	je	.L76

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm3
	movsd	1 * SIZE(XX), %xmm4
	addl	INCX, XX

	andpd	%xmm7, %xmm1
	andpd	%xmm7, %xmm2
	andpd	%xmm7, %xmm3
	andpd	%xmm7, %xmm4

	addpd	%xmm2,  %xmm1
	addpd	%xmm4,  %xmm3
	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	ALIGN_3

.L76:
	incl	RET
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
