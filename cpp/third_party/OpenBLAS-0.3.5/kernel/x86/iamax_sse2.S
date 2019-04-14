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
#ifdef USE_ABS
	pxor	%xmm7, %xmm7
#endif
	xor	RET, RET
	testl	M, M
	jle	.L999
	leal	(, INCX, SIZE), INCX
	testl	INCX, INCX
	jle	.L999

	movl	M, MM
	movl	X, XX

#ifdef USE_ABS
	cmpeqpd	%xmm7, %xmm7
	psrlq	$1, %xmm7
#endif

	movsd	(XX), %xmm0
	addl	INCX, XX
	decl	MM
#ifdef USE_ABS
	andpd	 %xmm7, %xmm0
#endif
	unpcklpd %xmm0, %xmm0
	movapd	 %xmm0, %xmm1
	movapd	 %xmm0, %xmm2
	movapd	 %xmm0, %xmm3
	cmpl	$SIZE, INCX
	jne	.L80

/* Analigned Check */
	cmpl	$7, MM
	jle	.L50

	testl	$7, XX
	jne	.L50		# Purely Unaligned Mode

	testl	$15, XX		# Checking for 128bit align
	je	.L05

	movsd	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	unpcklpd %xmm4, %xmm4
	maxpd	 %xmm4, %xmm3
	decl	MM
	addl	$SIZE, XX
	ALIGN_3

.L05:
	movl	MM,  I
	sarl	$4, I
	jle	.L15
	ALIGN_4

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movapd	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movapd	2 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1

	movapd	4 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2

	movapd	6 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE + 64) - PREOFFSET(XX)
#endif

	movapd	8 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movapd	10 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1

	movapd	12 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2

	movapd	14 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3

	addl	$16 * SIZE, XX
	decl	I
	jg	.L11
	ALIGN_4

.L15:
	andl	$15,  MM
	jle	.L20

	testl	$8, MM
	je	.L16

	movapd	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movapd	2 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1

	movapd	4 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2

	movapd	6 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3
	addl	$8 * SIZE, XX
	ALIGN_3

.L16:
	testl	$4, MM
	je	.L17

	movapd	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movapd	2 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1
	addl	$4 * SIZE, XX
	ALIGN_3

.L17:
	testl	$2, MM
	je	.L18

	movapd	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2
	addl	$2 * SIZE, XX

.L18:
	testl	$1, MM
	je	.L20

	movsd	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	unpcklpd  %xmm4, %xmm4
	maxpd	%xmm4, %xmm3
	ALIGN_3

/* Finding Index */
.L20:
	movl	X, XX
	movl	M, MM

	maxpd	 %xmm1, %xmm0
	maxpd	 %xmm3, %xmm2
	maxpd	 %xmm2, %xmm0
	movapd	 %xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	 %xmm1, %xmm0
	unpcklpd %xmm0, %xmm0

	testl	$15, XX		# Checking for 128bit align
	je	.L21

	movsd	0 * SIZE(XX), %xmm1
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
#endif
	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	addl	$SIZE, XX
	decl	MM
	ALIGN_3

.L21:
	movl	MM,  I
	sarl	$3, I
	jle	.L25
	ALIGN_4

.L22:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movapd	0 * SIZE(XX), %xmm1
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
#endif
	cmpeqpd	%xmm0, %xmm1

	movapd	2 * SIZE(XX), %xmm2
#ifdef USE_ABS
	andpd	 %xmm7, %xmm2
#endif
	cmpeqpd	%xmm0, %xmm2

	movapd	4 * SIZE(XX), %xmm3
#ifdef USE_ABS
	andpd	 %xmm7, %xmm3
#endif
	cmpeqpd	%xmm0, %xmm3

	movapd	6 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	cmpeqpd	%xmm0, %xmm4

	orpd	%xmm2, %xmm1
	orpd	%xmm4, %xmm3
	orpd	%xmm3, %xmm1
	movmskpd %xmm1, TEMP
	testl	 $3, TEMP
	jne	 .L23

	addl	$8 * SIZE, XX
	addl	$8, RET
	decl	I
	jg	.L22
	jmp	.L25
	ALIGN_4

.L23:
	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	movsd	2 * SIZE(XX), %xmm3
	movsd	3 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
	andpd	 %xmm7, %xmm3
	andpd	 %xmm7, %xmm4
#endif

	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm4
	je	.L999

	movsd	4 * SIZE(XX), %xmm1
	movsd	5 * SIZE(XX), %xmm2
	movsd	6 * SIZE(XX), %xmm3

#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
	andpd	 %xmm7, %xmm3
#endif

	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incl	RET
	jmp	.L999
	ALIGN_3

.L25:
	testl	$4, MM
	je	.L27

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	movsd	2 * SIZE(XX), %xmm3
	movsd	3 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
	andpd	 %xmm7, %xmm3
	andpd	 %xmm7, %xmm4
#endif
	addl	$4 * SIZE, XX
	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L27:
	testl	$2, MM
	je	.L28

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
#endif
	addl	$2 * SIZE, XX
	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L28:
	incl	RET
	jmp	.L999
	ALIGN_3

.L50:
/* Unaligned Mode */
	movl	MM,  I
	sarl	$4, I
	jle	.L55
	ALIGN_4

.L51:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movsd	0 * SIZE(XX), %xmm4
	movhpd	1 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	2 * SIZE(XX), %xmm4
	movhpd	3 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1

	movsd	4 * SIZE(XX), %xmm4
	movhpd	5 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2

	movsd	6 * SIZE(XX), %xmm4
	movhpd	7 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE + 64) - PREOFFSET(XX)
#endif

	movsd	8 * SIZE(XX), %xmm4
	movhpd	9 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	10 * SIZE(XX), %xmm4
	movhpd	11 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1

	movsd	12 * SIZE(XX), %xmm4
	movhpd	13 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2

	movsd	14 * SIZE(XX), %xmm4
	movhpd	15 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3

	addl	$16 * SIZE, XX
	decl	I
	jg	.L51
	ALIGN_4

.L55:
	andl	$15,  MM
	jle	.L60

	testl	$8, MM
	je	.L56

	movsd	0 * SIZE(XX), %xmm4
	movhpd	1 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	2 * SIZE(XX), %xmm4
	movhpd	3 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1

	movsd	4 * SIZE(XX), %xmm4
	movhpd	5 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2

	movsd	6 * SIZE(XX), %xmm4
	movhpd	7 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3

	addl	$8 * SIZE, XX
	ALIGN_3

.L56:
	testl	$4, MM
	je	.L57

	movsd	0 * SIZE(XX), %xmm4
	movhpd	1 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	2 * SIZE(XX), %xmm4
	movhpd	3 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1
	addl	$4 * SIZE, XX
	ALIGN_3

.L57:
	testl	$2, MM
	je	.L58

	movsd	0 * SIZE(XX), %xmm4
	movhpd	1 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2
	addl	$2 * SIZE, XX

.L58:
	testl	$1, MM
	je	.L60

	movsd	0 * SIZE(XX), %xmm4
	unpcklpd  %xmm4, %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3
	ALIGN_3

.L60:
	movl	X, XX
	movl	M, MM

	maxpd	 %xmm1, %xmm0
	maxpd	 %xmm3, %xmm2
	maxpd	 %xmm2, %xmm0
	movapd	 %xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	 %xmm1, %xmm0
	unpcklpd %xmm0, %xmm0

	movl	MM,  I
	sarl	$3, I
	jle	.L65
	ALIGN_4

.L62:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movsd	0 * SIZE(XX), %xmm1
	movhpd	1 * SIZE(XX), %xmm1
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
#endif
	cmpeqpd	%xmm0, %xmm1

	movsd	2 * SIZE(XX), %xmm2
	movhpd	3 * SIZE(XX), %xmm2
#ifdef USE_ABS
	andpd	 %xmm7, %xmm2
#endif
	cmpeqpd	%xmm0, %xmm2

	movsd	4 * SIZE(XX), %xmm3
	movhpd	5 * SIZE(XX), %xmm3
#ifdef USE_ABS
	andpd	 %xmm7, %xmm3
#endif
	cmpeqpd	%xmm0, %xmm3

	movsd	6 * SIZE(XX), %xmm4
	movhpd	7 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	cmpeqpd	%xmm0, %xmm4

	orpd	%xmm2, %xmm1
	orpd	%xmm4, %xmm3
	orpd	%xmm3, %xmm1
	movmskpd %xmm1, TEMP
	testl	 $3, TEMP
	jne	 .L63

	addl	$8 * SIZE, XX
	addl	$8, RET
	decl	I
	jg	.L62
	jmp	.L65
	ALIGN_4

.L63:
	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	movsd	2 * SIZE(XX), %xmm3
	movsd	3 * SIZE(XX), %xmm4

#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
	andpd	 %xmm7, %xmm3
	andpd	 %xmm7, %xmm4
#endif

	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm4
	je	.L999
	incl	RET

	movsd	4 * SIZE(XX), %xmm1
	movsd	5 * SIZE(XX), %xmm2
	movsd	6 * SIZE(XX), %xmm3

#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
	andpd	 %xmm7, %xmm3
#endif

	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incl	RET
	jmp	.L999
	ALIGN_3

.L65:
	testl	$4, MM
	je	.L67

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
	movsd	2 * SIZE(XX), %xmm3
	movsd	3 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
	andpd	 %xmm7, %xmm3
	andpd	 %xmm7, %xmm4
#endif
	addl	$4 * SIZE, XX
	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L67:
	testl	$2, MM
	je	.L68

	movsd	0 * SIZE(XX), %xmm1
	movsd	1 * SIZE(XX), %xmm2
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
#endif
	addl	$2 * SIZE, XX
	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L68:
	incl	RET
	jmp	.L999
	ALIGN_4

.L80:
	movl	MM,  I
	sarl	$4, I
	jle	.L85
	ALIGN_4

.L81:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	%xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	%xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	%xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3

	decl	I
	jg	.L81
	ALIGN_4

.L85:
	andl	$15,  MM
	jle	.L90

	testl	$8, MM
	je	.L86

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	%xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	%xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	%xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	%xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm3
	ALIGN_3

.L86:
	testl	$4, MM
	je	.L87

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	%xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	%xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm1
	ALIGN_3

.L87:
	testl	$2, MM
	je	.L88

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	maxpd	%xmm4, %xmm2
	ALIGN_3

.L88:
	testl	$1, MM
	je	.L90

	movsd	0 * SIZE(XX), %xmm4
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	unpcklpd  %xmm4, %xmm4
	maxpd	%xmm4, %xmm3
	ALIGN_4

.L90:
	movl	X, XX
	movl	M, MM

	maxpd	 %xmm1, %xmm0
	maxpd	 %xmm3, %xmm2
	maxpd	 %xmm2, %xmm0
	movapd	 %xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	 %xmm1, %xmm0
	unpcklpd %xmm0, %xmm0

	movl	MM,  I
	sarl	$3, I
	jle	.L95
	ALIGN_4

.L92:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(XX)
#endif

	movsd	0 * SIZE(XX), %xmm1
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm1
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
#endif
	cmpeqpd	%xmm0, %xmm1

	movsd	0 * SIZE(XX), %xmm2
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm2
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm2
#endif
	cmpeqpd	%xmm0, %xmm2

	movsd	0 * SIZE(XX), %xmm3
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm3
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm3
#endif
	cmpeqpd	%xmm0, %xmm3

	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
	movhpd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm4
#endif
	cmpeqpd	%xmm0, %xmm4

	orpd	%xmm2, %xmm1
	orpd	%xmm4, %xmm3
	orpd	%xmm3, %xmm1
	movmskpd %xmm1, TEMP
	testl	 $3, TEMP
	jne	 .L93

	addl	$8, RET
	decl	I
	jg	.L92
	jmp	.L95
	ALIGN_4

.L93:
	leal	(, INCX, 8), TEMP
	subl	TEMP, XX

	movsd	0 * SIZE(XX), %xmm1
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm2
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm3
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
	andpd	 %xmm7, %xmm3
	andpd	 %xmm7, %xmm4
#endif
	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm4
	je	.L999

	movsd	0 * SIZE(XX), %xmm1
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm2
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm3
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
	andpd	 %xmm7, %xmm3
#endif

	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incl	RET
	jmp	.L999
	ALIGN_3

.L95:
	testl	$4, MM
	je	.L97

	movsd	0 * SIZE(XX), %xmm1
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm2
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm3
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm4
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
	andpd	 %xmm7, %xmm3
	andpd	 %xmm7, %xmm4
#endif
	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L97:
	testl	$2, MM
	je	.L98

	movsd	0 * SIZE(XX), %xmm1
	addl	INCX, XX
	movsd	0 * SIZE(XX), %xmm2
	addl	INCX, XX
#ifdef USE_ABS
	andpd	 %xmm7, %xmm1
	andpd	 %xmm7, %xmm2
#endif
	incl	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incl	RET
	comisd	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L98:
	incl	RET
	ALIGN_3

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
