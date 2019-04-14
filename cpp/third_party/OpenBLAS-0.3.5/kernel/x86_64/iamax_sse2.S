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

#define M	ARG1	/* rdi */
#define X	ARG2	/* rsi */
#define INCX	ARG3	/* rdx */

#define RET	%rax
#define I	ARG4
#define XX	%r10
#define	MM	%r11

#ifdef USE_MIN
#define maxpd	minpd
#define maxsd	minsd
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

	SAVEREGISTERS

	pxor	%xmm0, %xmm0
	xor	RET, RET
	testq	M, M
	jle	.L999
	leaq	(, INCX, SIZE), INCX
	testq	INCX, INCX
	jle	.L999

	movq	M, MM
	movq	X, XX

#ifdef USE_ABS
	pcmpeqb	%xmm15, %xmm15
	psrlq	$1, %xmm15
#endif

	movsd	(X), %xmm0
	addq	INCX, X
	decq	M
#ifdef USE_ABS
	andpd	 %xmm15, %xmm0
#endif
	unpcklpd %xmm0, %xmm0
	movapd	 %xmm0, %xmm1
	movapd	 %xmm0, %xmm2
	movapd	 %xmm0, %xmm3
	cmpq	$SIZE, INCX
	jne	.L80

/* Analigned Check */
	cmpq	$7, M
	jle	.L50

	testq	$7, X
	jne	.L50		# Purely Unaligned Mode

	testq	$15, X		# Checking for 128bit align
	je	.L05

	movsd	0 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	unpcklpd  %xmm4, %xmm4
	maxpd	  %xmm4, %xmm3
	decq	M
	addq	$SIZE, X
	ALIGN_3

.L05:
	movq	M,  I
	sarq	$4, I
	jle	.L15
	ALIGN_4

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movapd	0 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movapd	2 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movapd	4 * SIZE(X), %xmm6
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movapd	6 * SIZE(X), %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movapd	8 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movapd	10 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movapd	12 * SIZE(X), %xmm6
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movapd	14 * SIZE(X), %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

	addq	$16 * SIZE, X
	decq	I
	jg	.L11
	ALIGN_4

.L15:
	andq	$15,  M
	jle	.L20

	testq	$8, M
	je	.L16

	movapd	0 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movapd	2 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movapd	4 * SIZE(X), %xmm6
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movapd	6 * SIZE(X), %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3
	addq	$8 * SIZE, X
	ALIGN_3

.L16:
	testq	$4, M
	je	.L17

	movapd	0 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movapd	2 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	addq	$4 * SIZE, X
	ALIGN_3

.L17:
	testq	$2, M
	je	.L18

	movapd	0 * SIZE(X), %xmm6
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2
	addq	$2 * SIZE, X

.L18:
	testq	$1, M
	je	.L20

	movsd	0 * SIZE(X), %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	unpcklpd  %xmm7, %xmm7
	maxpd	%xmm7, %xmm3
	ALIGN_3

/* Finding Index */
.L20:
	movq	XX, X
	movq	MM, M

	maxpd	 %xmm1, %xmm0
	maxpd	 %xmm3, %xmm2
	maxpd	 %xmm2, %xmm0
	movapd	 %xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	 %xmm1, %xmm0
	unpcklpd %xmm0, %xmm0
	ALIGN_3

	testq	$15, X		# Checking for 128bit align
	je	.L21

	movsd	0 * SIZE(X), %xmm1
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
#endif
	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	addq	$SIZE, X
	decq	M
	ALIGN_3

.L21:
	movq	M,  I
	sarq	$3, I
	jle	.L25
	ALIGN_4

.L22:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movapd	0 * SIZE(X), %xmm1
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
#endif
	cmpeqpd	%xmm0, %xmm1

	movapd	2 * SIZE(X), %xmm3
#ifdef USE_ABS
	andpd	 %xmm15, %xmm3
#endif
	cmpeqpd	%xmm0, %xmm3

	movapd	4 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	cmpeqpd	%xmm0, %xmm5

	movapd	6 * SIZE(X), %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	cmpeqpd	%xmm0, %xmm7

	orpd	%xmm3, %xmm1
	orpd	%xmm7, %xmm5
	orpd	%xmm5, %xmm1
#ifndef C_SUN
	movmskpd %xmm1, %r11
#else
	.byte	0x66
	.long	0xd9500f4c
#endif
	testq	 $3, %r11
	jne	 .L23

	addq	$8 * SIZE, X
	addq	$8, RET
	decq	I
	jg	.L22
	jmp	.L25
	ALIGN_4

.L23:
	movsd	0 * SIZE(X), %xmm1
	movsd	1 * SIZE(X), %xmm2
	movsd	2 * SIZE(X), %xmm3
	movsd	3 * SIZE(X), %xmm4
	movsd	4 * SIZE(X), %xmm5
	movsd	5 * SIZE(X), %xmm6
	movsd	6 * SIZE(X), %xmm7
	movsd	7 * SIZE(X), %xmm8
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
	andpd	 %xmm15, %xmm2
	andpd	 %xmm15, %xmm3
	andpd	 %xmm15, %xmm4
	andpd	 %xmm15, %xmm5
	andpd	 %xmm15, %xmm6
	andpd	 %xmm15, %xmm7
	andpd	 %xmm15, %xmm8
#endif

	addq	$8 * SIZE, X

	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm4
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm5
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm6
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm7
	je	.L999
	incq	RET
	jmp	.L999
	ALIGN_3

.L25:
	testq	$4, M
	je	.L27

	movsd	0 * SIZE(X), %xmm1
	movsd	1 * SIZE(X), %xmm2
	movsd	2 * SIZE(X), %xmm3
	movsd	3 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
	andpd	 %xmm15, %xmm2
	andpd	 %xmm15, %xmm3
	andpd	 %xmm15, %xmm4
#endif
	addq	$4 * SIZE, X
	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L27:
	testq	$2, M
	je	.L28

	movsd	0 * SIZE(X), %xmm1
	movsd	1 * SIZE(X), %xmm2
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
	andpd	 %xmm15, %xmm2
#endif
	addq	$2 * SIZE, X
	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L28:
	incq	RET
	jmp	.L999
	ALIGN_3

/* Unaligned Mode */
.L50:
	movq	M,  I
	sarq	$4, I
	jle	.L55
	ALIGN_4

.L51:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(X), %xmm4
	movhpd	1 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	2 * SIZE(X), %xmm5
	movhpd	3 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	4 * SIZE(X), %xmm6
	movhpd	5 * SIZE(X), %xmm6
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movsd	6 * SIZE(X), %xmm7
	movhpd	7 * SIZE(X), %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movsd	8 * SIZE(X), %xmm4
	movhpd	9 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	10 * SIZE(X), %xmm5
	movhpd	11 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	12 * SIZE(X), %xmm6
	movhpd	13 * SIZE(X), %xmm6
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movsd	14 * SIZE(X), %xmm7
	movhpd	15 * SIZE(X), %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

	addq	$16 * SIZE, X
	decq	I
	jg	.L51
	ALIGN_4

.L55:
	andq	$15,  M
	jle	.L60

	testq	$8, M
	je	.L56

	movsd	0 * SIZE(X), %xmm4
	movhpd	1 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	2 * SIZE(X), %xmm5
	movhpd	3 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	4 * SIZE(X), %xmm6
	movhpd	5 * SIZE(X), %xmm6
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movsd	6 * SIZE(X), %xmm7
	movhpd	7 * SIZE(X), %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

	addq	$8 * SIZE, X
	ALIGN_3

.L56:
	testq	$4, M
	je	.L57

	movsd	0 * SIZE(X), %xmm4
	movhpd	1 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	2 * SIZE(X), %xmm5
	movhpd	3 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	addq	$4 * SIZE, X
	ALIGN_3

.L57:
	testq	$2, M
	je	.L58

	movsd	0 * SIZE(X), %xmm6
	movhpd	1 * SIZE(X), %xmm6
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2
	addq	$2 * SIZE, X

.L58:
	testq	$1, M
	je	.L60

	movsd	0 * SIZE(X), %xmm7
	unpcklpd  %xmm7, %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3
	ALIGN_3

.L60:
	movq	XX, X
	movq	MM, M

	maxpd	 %xmm1, %xmm0
	maxpd	 %xmm3, %xmm2
	maxpd	 %xmm2, %xmm0
	movapd	 %xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	 %xmm1, %xmm0
	unpcklpd %xmm0, %xmm0

	movq	M,  I
	sarq	$3, I
	jle	.L65
	ALIGN_4

.L62:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(X), %xmm1
	movhpd	1 * SIZE(X), %xmm1
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
#endif
	cmpeqpd	%xmm0, %xmm1

	movsd	2 * SIZE(X), %xmm3
	movhpd	3 * SIZE(X), %xmm3
#ifdef USE_ABS
	andpd	 %xmm15, %xmm3
#endif
	cmpeqpd	%xmm0, %xmm3

	movsd	4 * SIZE(X), %xmm5
	movhpd	5 * SIZE(X), %xmm5
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	cmpeqpd	%xmm0, %xmm5

	movsd	6 * SIZE(X), %xmm7
	movhpd	7 * SIZE(X), %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	cmpeqpd	%xmm0, %xmm7

	orpd	%xmm3, %xmm1
	orpd	%xmm7, %xmm5
	orpd	%xmm5, %xmm1
#ifndef C_SUN
	movmskpd %xmm1, %r11
#else
	.byte	0x66
	.long	0xd9500f4c
#endif
	testq	 $3, %r11
	jne	 .L63

	addq	$8 * SIZE, X
	addq	$8, RET
	decq	I
	jg	.L62
	jmp	.L65
	ALIGN_4

.L63:
	movsd	0 * SIZE(X), %xmm1
	movsd	1 * SIZE(X), %xmm2
	movsd	2 * SIZE(X), %xmm3
	movsd	3 * SIZE(X), %xmm4
	movsd	4 * SIZE(X), %xmm5
	movsd	5 * SIZE(X), %xmm6
	movsd	6 * SIZE(X), %xmm7
	movsd	7 * SIZE(X), %xmm8
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
	andpd	 %xmm15, %xmm2
	andpd	 %xmm15, %xmm3
	andpd	 %xmm15, %xmm4
	andpd	 %xmm15, %xmm5
	andpd	 %xmm15, %xmm6
	andpd	 %xmm15, %xmm7
	andpd	 %xmm15, %xmm8
#endif

	addq	$8 * SIZE, X

	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm4
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm5
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm6
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm7
	je	.L999
	incq	RET
	jmp	.L999
	ALIGN_3

.L65:
	testq	$4, M
	je	.L67

	movsd	0 * SIZE(X), %xmm1
	movsd	1 * SIZE(X), %xmm2
	movsd	2 * SIZE(X), %xmm3
	movsd	3 * SIZE(X), %xmm4
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
	andpd	 %xmm15, %xmm2
	andpd	 %xmm15, %xmm3
	andpd	 %xmm15, %xmm4
#endif
	addq	$4 * SIZE, X
	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L67:
	testq	$2, M
	je	.L68

	movsd	0 * SIZE(X), %xmm1
	movsd	1 * SIZE(X), %xmm2
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
	andpd	 %xmm15, %xmm2
#endif
	addq	$2 * SIZE, X
	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L68:
	incq	RET
	jmp	.L999
	ALIGN_4

.L80:
	movq	M,  I
	sarq	$4, I
	jle	.L85
	ALIGN_4

.L81:
	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	0 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	0 * SIZE(X), %xmm6
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movsd	0 * SIZE(X), %xmm7
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	0 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	0 * SIZE(X), %xmm6
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movsd	0 * SIZE(X), %xmm7
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

	decq	I
	jg	.L81
	ALIGN_4

.L85:
	andq	$15,  M
	jle	.L90

	testq	$8, M
	je	.L86

	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	0 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	0 * SIZE(X), %xmm6
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movsd	0 * SIZE(X), %xmm7
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3
	ALIGN_3

.L86:
	testq	$4, M
	je	.L87

	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	0 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	ALIGN_3

.L87:
	testq	$2, M
	je	.L88

	movsd	0 * SIZE(X), %xmm6
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2
	ALIGN_3

.L88:
	testq	$1, M
	je	.L90

	movsd	0 * SIZE(X), %xmm7
	unpcklpd  %xmm7, %xmm7
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

	maxpd	%xmm1, %xmm0
	maxpd	%xmm3, %xmm2
	maxpd	%xmm2, %xmm0
	movapd	%xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	%xmm1, %xmm0
	ALIGN_4

.L90:
	movq	XX, X
	movq	MM, M

	maxpd	 %xmm1, %xmm0
	maxpd	 %xmm3, %xmm2
	maxpd	 %xmm2, %xmm0
	movapd	 %xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	 %xmm1, %xmm0
	unpcklpd %xmm0, %xmm0

	movq	M,  I
	sarq	$3, I
	jle	.L95
	ALIGN_4

.L92:
	movsd	0 * SIZE(X), %xmm1
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm1
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
#endif
	cmpeqpd	%xmm0, %xmm1

	movsd	0 * SIZE(X), %xmm3
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm3
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm3
#endif
	cmpeqpd	%xmm0, %xmm3

	movsd	0 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm5
#endif
	cmpeqpd	%xmm0, %xmm5

	movsd	0 * SIZE(X), %xmm7
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm7
#endif
	cmpeqpd	%xmm0, %xmm7

	orpd	%xmm3, %xmm1
	orpd	%xmm7, %xmm5
	orpd	%xmm5, %xmm1
#ifndef C_SUN
	movmskpd %xmm1, %r11
#else
	.byte	0x66
	.long	0xd9500f4c
#endif
	testq	 $3, %r11
	jne	 .L93

	addq	$8, RET
	decq	I
	jg	.L92
	jmp	.L95
	ALIGN_4

.L93:
	subq	INCX, X
	movsd	0 * SIZE(X), %xmm8
	subq	INCX, X
	movsd	0 * SIZE(X), %xmm7
	subq	INCX, X
	movsd	0 * SIZE(X), %xmm6
	subq	INCX, X
	movsd	0 * SIZE(X), %xmm5
	subq	INCX, X
	movsd	0 * SIZE(X), %xmm4
	subq	INCX, X
	movsd	0 * SIZE(X), %xmm3
	subq	INCX, X
	movsd	0 * SIZE(X), %xmm2
	subq	INCX, X
	movsd	0 * SIZE(X), %xmm1
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
	andpd	 %xmm15, %xmm2
	andpd	 %xmm15, %xmm3
	andpd	 %xmm15, %xmm4
	andpd	 %xmm15, %xmm5
	andpd	 %xmm15, %xmm6
	andpd	 %xmm15, %xmm7
	andpd	 %xmm15, %xmm8
#endif

	addq	$8 * SIZE, X

	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm4
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm5
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm6
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm7
	je	.L999
	incq	RET
	jmp	.L999
	ALIGN_3

.L95:
	testq	$4, M
	je	.L97

	movsd	0 * SIZE(X), %xmm1
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm2
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm3
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
	andpd	 %xmm15, %xmm2
	andpd	 %xmm15, %xmm3
	andpd	 %xmm15, %xmm4
#endif
	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm2
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm3
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L97:
	testq	$2, M
	je	.L98

	movsd	0 * SIZE(X), %xmm1
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm2
	addq	INCX, X
#ifdef USE_ABS
	andpd	 %xmm15, %xmm1
	andpd	 %xmm15, %xmm2
#endif
	incq	RET
	comisd	%xmm0, %xmm1
	je	.L999
	incq	RET
	comisd	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L98:
	incq	RET
	ALIGN_3

.L999:
	RESTOREREGISTERS

	ret

	EPILOGUE

