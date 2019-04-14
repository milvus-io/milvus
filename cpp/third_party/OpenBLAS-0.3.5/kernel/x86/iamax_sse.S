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
#define maxps	minps
#define maxss	minss
#endif

#ifndef HAVE_SSE2
#define pxor	xorps
#define movsd	movlps
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

	pxor	%xmm0, %xmm0		/* Return Value(Float) */
#ifdef USE_ABS
	pxor	%xmm7, %xmm7		/* Generate USE_ABS */
#endif
	xor	RET, RET		/* Return Value(Int)   */
	testl	M, M
	jle	.L999
	leal	(, INCX, SIZE), INCX
	testl	INCX, INCX
	jle	.L999

	movl	M, MM
	movl	X, XX

#ifdef USE_ABS
#ifndef HAVE_SSE2
	subl	$8, %esp
	movl	$0x7fffffff, (%esp)
	movss	(%esp), %xmm7
	shufps	$0, %xmm7, %xmm7
	addl	$8, %esp
#else
	cmpeqps	%xmm7, %xmm7
	psrld	$1, %xmm7		/* Generate USE_ABS */
#endif
#endif

	movss	(XX), %xmm0
	addl	INCX, XX
	decl	MM
	shufps	$0, %xmm0, %xmm0
#ifdef USE_ABS
	andps	%xmm7, %xmm0
#endif
	movaps	%xmm0, %xmm1
	movaps	%xmm0, %xmm2
	movaps	%xmm0, %xmm3		/* Generating "seed value" */
	cmpl	$SIZE, INCX
	jne	.L80			/* Incx != 1 goto L80 */

/* Analigned Check */
	testl	$3, XX			/* 00000011 */
	jne	.L30			/* Purely Unaligned Mode */

	cmpl	$8, MM
	jle	.L30			/* if M <= 8 goto Unaligned mode */

	testl	$4, XX			/* bit test 000100 */
	je	.L05

	movss	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	  %xmm4, %xmm0
	decl	MM
	addl	$SIZE, XX
	ALIGN_3

.L05:
	testl	$8, XX
	je	.L06

	movsd	0 * SIZE(XX), %xmm4
	unpcklps  %xmm4, %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	  %xmm4, %xmm1
	subl	$2, MM
	addl	$2 * SIZE, XX
	ALIGN_3

.L06:
	movl	MM,  I
	sarl	$4, I
	jle	.L15
	ALIGN_4

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movaps	 0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movaps	 4 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm1

	movaps	 8 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm2

	movaps	12 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm3

	addl	$16 * SIZE, XX
	decl	I
	jg	.L11
	ALIGN_4

.L15:
	andl	$15,  MM
	jle	.L20

	testl	$8, MM
	je	.L16

	movaps	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movaps	4 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm1
	addl	$8 * SIZE, XX
	ALIGN_3

.L16:
	testl	$4, MM
	je	.L17

	movaps	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm2
	addl	$4 * SIZE, XX
	ALIGN_3

.L17:
	testl	$2, MM
	je	.L18

	movsd	0 * SIZE(XX), %xmm4
	unpcklps %xmm4, %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm3
	addl	$2 * SIZE, XX

.L18:
	testl	$1, MM
	je	.L20

	movss	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm0
	ALIGN_3

.L20:
	movl	X, XX
	movl	M, MM

	maxps	%xmm1, %xmm0
	maxps	%xmm3, %xmm2
	maxps	%xmm2, %xmm0
	movaps	%xmm0, %xmm1
	movhlps %xmm0, %xmm0
	maxps	%xmm1, %xmm0
	movaps	%xmm0, %xmm1
	shufps	$1, %xmm0, %xmm0
	maxss	%xmm1, %xmm0
	shufps	$0, %xmm0, %xmm0

	testl	$4, XX
	je	.L21

	movss	0 * SIZE(XX), %xmm1

	decl	MM
	addl	$SIZE, XX

#ifdef USE_ABS
	andps	 %xmm7, %xmm1
#endif
	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	ALIGN_3

.L21:
	testl	$8, XX
	je	.L22

	movss	0 * SIZE(XX), %xmm1
	movss	1 * SIZE(XX), %xmm2

	subl	$2, MM
	addl	$2 * SIZE, XX

#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
#endif
	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L22:
	movl	MM,  I
	sarl	$3, I
	jle	.L25
	ALIGN_4

.L23:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movaps	0 * SIZE(XX), %xmm1
#ifdef USE_ABS
	andps	 %xmm7, %xmm1
#endif
	cmpeqps	%xmm0, %xmm1

	movaps	4 * SIZE(XX), %xmm2
#ifdef USE_ABS
	andps	 %xmm7, %xmm2
#endif
	cmpeqps	%xmm0, %xmm2

	orps	%xmm2, %xmm1
	movmskps %xmm1, TEMP
	testl	 $15, TEMP
	jne	 .L24

	addl	$8 * SIZE, XX
	addl	$8, RET
	decl	I
	jg	.L23
	jmp	.L25
	ALIGN_3

.L24:
	movss	0 * SIZE(XX), %xmm1
	movss	1 * SIZE(XX), %xmm2
	movss	2 * SIZE(XX), %xmm3
	movss	3 * SIZE(XX), %xmm4

#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
	andps	 %xmm7, %xmm3
	andps	 %xmm7, %xmm4
#endif

	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm4
	je	.L999

	movss	4 * SIZE(XX), %xmm1
	movss	5 * SIZE(XX), %xmm2
	movss	6 * SIZE(XX), %xmm3

#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
	andps	 %xmm7, %xmm3
#endif

	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incl	RET
	jmp	.L999
	ALIGN_4

.L25:
	testl	$4, MM
	je	.L26

	movss	0 * SIZE(XX), %xmm1
	movss	1 * SIZE(XX), %xmm2
	movss	2 * SIZE(XX), %xmm3
	movss	3 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
	andps	 %xmm7, %xmm3
	andps	 %xmm7, %xmm4
#endif
	addl	$4 * SIZE, XX
	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L26:
	testl	$2, MM
	je	.L27

	movss	0 * SIZE(XX), %xmm1
	movss	1 * SIZE(XX), %xmm2
#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
#endif
	addl	$2 * SIZE, XX
	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L27:
	incl	RET
	jmp	.L999
	ALIGN_3

/* Unaligned Mode */
.L30:
	movl	MM,  I
	sarl	$4, I
	jle	.L35
	ALIGN_4

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movsd	 0 * SIZE(XX), %xmm4
	movhps	 2 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movsd	 4 * SIZE(XX), %xmm4
	movhps	 6 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm1

	movsd	 8 * SIZE(XX), %xmm4
	movhps	10 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm2

	movsd	12 * SIZE(XX), %xmm4
	movhps	14 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm3

	addl	$16 * SIZE, XX
	decl	I
	jg	.L31
	ALIGN_4

.L35:
	andl	$15,  MM
	jle	.L40

	testl	$8, MM
	je	.L36

	movsd	0 * SIZE(XX), %xmm4
	movhps	2 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movsd	4 * SIZE(XX), %xmm4
	movhps	6 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm1

	addl	$8 * SIZE, XX
	ALIGN_3

.L36:
	testl	$4, MM
	je	.L37

	movsd	0 * SIZE(XX), %xmm4
	movhps	2 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm2
	addl	$4 * SIZE, XX
	ALIGN_3

.L37:
	testl	$2, MM
	je	.L38

	movsd	0 * SIZE(XX), %xmm4
	unpcklps %xmm4, %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxps	%xmm4, %xmm3
	addl	$2 * SIZE, XX

.L38:
	testl	$1, MM
	je	.L40

	movss	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm0
	jmp	.L40
	ALIGN_4

.L40:
	movl	X, XX
	movl	M, MM

	maxps	%xmm1, %xmm0
	maxps	%xmm3, %xmm2
	maxps	%xmm2, %xmm0
	movaps	%xmm0, %xmm1
	movhlps %xmm0, %xmm0
	maxps	%xmm1, %xmm0
	movaps	%xmm0, %xmm1
	shufps	$1, %xmm0, %xmm0
	maxss	%xmm1, %xmm0
	shufps	$0, %xmm0, %xmm0

	movl	MM,  I
	sarl	$3, I
	jle	.L45
	ALIGN_4

.L43:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movsd	0 * SIZE(XX), %xmm1
	movhps	2 * SIZE(XX), %xmm1
#ifdef USE_ABS
	andps	 %xmm7, %xmm1
#endif
	cmpeqps	%xmm0, %xmm1

	movsd	4 * SIZE(XX), %xmm2
	movhps	6 * SIZE(XX), %xmm2
#ifdef USE_ABS
	andps	 %xmm7, %xmm2
#endif
	cmpeqps	%xmm0, %xmm2

	orps	%xmm2, %xmm1
	movmskps %xmm1, TEMP
	testl	 $15, TEMP
	jne	 .L44

	addl	$8 * SIZE, XX
	addl	$8, RET
	decl	I
	jg	.L43
	jmp	.L45
	ALIGN_3

.L44:
	movss	0 * SIZE(XX), %xmm1
	movss	1 * SIZE(XX), %xmm2
	movss	2 * SIZE(XX), %xmm3
	movss	3 * SIZE(XX), %xmm4

#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
	andps	 %xmm7, %xmm3
	andps	 %xmm7, %xmm4
#endif

	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm4
	je	.L999

	movss	4 * SIZE(XX), %xmm1
	movss	5 * SIZE(XX), %xmm2
	movss	6 * SIZE(XX), %xmm3

#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
	andps	 %xmm7, %xmm3
#endif

	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incl	RET
	jmp	.L999
	ALIGN_4

.L45:
	testl	$4, MM
	je	.L46

	movss	0 * SIZE(XX), %xmm1
	movss	1 * SIZE(XX), %xmm2
	movss	2 * SIZE(XX), %xmm3
	movss	3 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
	andps	 %xmm7, %xmm3
	andps	 %xmm7, %xmm4
#endif
	addl	$4 * SIZE, XX
	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L46:
	testl	$2, MM
	je	.L47

	movss	0 * SIZE(XX), %xmm1
	movss	1 * SIZE(XX), %xmm2
#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
#endif
	addl	$2 * SIZE, XX
	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L47:
	incl	RET
	jmp	.L999
	ALIGN_3

.L80:
	movl	MM,  I
	sarl	$3, I
	jle	.L85
	ALIGN_4

.L81:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm1

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm2

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm3

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm1

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm2

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm3

	decl	I
	jg	.L81
	ALIGN_4

.L85:
	andl	$7,  MM
	jle	.L90

	testl	$4, MM
	je	.L86

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm1

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm2

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm3
	ALIGN_3

.L86:
	testl	$2, MM
	je	.L87

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm1
	ALIGN_3

.L87:
	testl	$1, MM
	je	.L90

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	%xmm7, %xmm4
#endif
	maxss	%xmm4, %xmm2
	ALIGN_4

.L90:
	movl	X, XX
	movl	M, MM

	maxss	%xmm1, %xmm0
	maxss	%xmm3, %xmm2
	maxss	%xmm2, %xmm0
	shufps	$0, %xmm0, %xmm0

	movl	MM,  I
	sarl	$2, I
	jle	.L96
	ALIGN_4

.L92:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movss	0 * SIZE(XX), %xmm1
	addl	INCX, XX
#ifdef USE_ABS
	andps	 %xmm7, %xmm1
#endif
	cmpeqss	%xmm0, %xmm1

	movss	0 * SIZE(XX), %xmm2
	addl	INCX, XX
#ifdef USE_ABS
	andps	 %xmm7, %xmm2
#endif
	cmpeqss	%xmm0, %xmm2

	movss	0 * SIZE(XX), %xmm3
	addl	INCX, XX
#ifdef USE_ABS
	andps	 %xmm7, %xmm3
#endif
	cmpeqss	%xmm0, %xmm3

	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andps	 %xmm7, %xmm4
#endif
	cmpeqss	%xmm0, %xmm4

	orps	%xmm2, %xmm1
	orps	%xmm4, %xmm3
	orps	%xmm3, %xmm1
	movmskps %xmm1, TEMP
	testl	 $15, TEMP
	jne	 .L93

	addl	$4, RET
	decl	I
	jg	.L92
	jmp	.L96
	ALIGN_3

.L93:
	leal	(, INCX, 4), TEMP
	subl	TEMP, XX

	movss	0 * SIZE(XX), %xmm1
	addl	INCX, XX
	movss	0 * SIZE(XX), %xmm2
	addl	INCX, XX
	movss	0 * SIZE(XX), %xmm3
	addl	INCX, XX
	movss	0 * SIZE(XX), %xmm4
	addl	INCX, XX

#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
	andps	 %xmm7, %xmm3
	andps	 %xmm7, %xmm4
#endif
	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L96:
	testl	$2, MM
	je	.L97

	movss	0 * SIZE(XX), %xmm1
	addl	INCX, XX
	movss	0 * SIZE(XX), %xmm2
	addl	INCX, XX
#ifdef USE_ABS
	andps	 %xmm7, %xmm1
	andps	 %xmm7, %xmm2
#endif
	incl	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incl	RET
	comiss	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L97:
	incl	RET
	ALIGN_3

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
