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
#define maxps	minps
#define maxss	minss
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

	SAVEREGISTERS

	pxor	%xmm0, %xmm0		/* Return Value(Float) */
	xor	RET, RET		/* Return Value(Int)   */
	testq	M, M
	jle	.L999
	leaq	(, INCX, SIZE), INCX
	testq	INCX, INCX
	jle	.L999

	movq	M, MM
	movq	X, XX

#ifdef USE_ABS
	pcmpeqb	%xmm15, %xmm15
	psrld	$1, %xmm15		/* Generate USE_ABS */
#endif

	movss	(X), %xmm0
	addq	INCX, X
	decq	M
	shufps	$0, %xmm0, %xmm0
#ifdef USE_ABS
	andps	%xmm15, %xmm0
#endif
	movaps	%xmm0, %xmm1
	movaps	%xmm0, %xmm2
	movaps	%xmm0, %xmm3		/* Generating "seed value" */
	cmpq	$SIZE, INCX
	jne	.L80			/* Incx != 1 goto L80 */

/* Analigned Check */
	testq	$3, X			/* 00000011 */
	jne	.L30			/* Purely Unaligned Mode */

	cmpq	$8, M
	jle	.L30			/* if M <= 8 goto Unaligned mode */

	testq	$4, X			/* bit test 000100 */
	je	.L05

	movss	0 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	  %xmm4, %xmm0
	decq	M
	addq	$SIZE, X
	ALIGN_3

.L05:
	testq	$8, X
	je	.L06

	movsd	0 * SIZE(X), %xmm4
	unpcklps  %xmm4, %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	  %xmm4, %xmm1
	subq	$2, M
	addq	$2 * SIZE, X
	ALIGN_3

.L06:
	movq	M,  I
	sarq	$4, I
	jle	.L15
	ALIGN_4

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	 0 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movaps	 4 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1

	movaps	 8 * SIZE(X), %xmm6
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxps	%xmm6, %xmm2

	movaps	12 * SIZE(X), %xmm7
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxps	%xmm7, %xmm3

	addq	$16 * SIZE, X
	decq	I
	jg	.L11
	ALIGN_4

.L15:
	andq	$15,  M
	jle	.L20

	testq	$8, M
	je	.L16

	movaps	0 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movaps	4 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1
	addq	$8 * SIZE, X
	ALIGN_3

.L16:
	testq	$4, M
	je	.L17

	movaps	0 * SIZE(X), %xmm6
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxps	%xmm6, %xmm2
	addq	$4 * SIZE, X
	ALIGN_3

.L17:
	testq	$2, M
	je	.L18

	movsd	0 * SIZE(X), %xmm7
	unpcklps %xmm7, %xmm7
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxps	%xmm7, %xmm3
	addq	$2 * SIZE, X

.L18:
	testq	$1, M
	je	.L20

	movss	0 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0
	ALIGN_3

.L20:
	movq	XX, X
	movq	MM, M

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

	testq	$4, X
	je	.L21

	movss	0 * SIZE(X), %xmm1

	decq	M
	addq	$SIZE, X

#ifdef USE_ABS
	andps	 %xmm15, %xmm1
#endif
	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	ALIGN_3

.L21:
	testq	$8, X
	je	.L22

	movss	0 * SIZE(X), %xmm1
	movss	1 * SIZE(X), %xmm2

	subq	$2, M
	addq	$2 * SIZE, X

#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
#endif
	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L22:
	movq	M,  I
	sarq	$3, I
	jle	.L25
	ALIGN_4

.L23:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	0 * SIZE(X), %xmm1
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
#endif
	cmpeqps	%xmm0, %xmm1

	movaps	4 * SIZE(X), %xmm3
#ifdef USE_ABS
	andps	 %xmm15, %xmm3
#endif
	cmpeqps	%xmm0, %xmm3

	orps	%xmm3, %xmm1
#ifndef C_SUN
	movmskps %xmm1, %r11
#else
	.long	0xd9500f4c
#endif
	testq	 $15, %r11
	jne	 .L24

	addq	$8 * SIZE, X
	addq	$8, RET
	decq	I
	jg	.L23
	jmp	.L25
	ALIGN_3

.L24:
	movss	0 * SIZE(X), %xmm1
	movss	1 * SIZE(X), %xmm2
	movss	2 * SIZE(X), %xmm3
	movss	3 * SIZE(X), %xmm4
	movss	4 * SIZE(X), %xmm5
	movss	5 * SIZE(X), %xmm6
	movss	6 * SIZE(X), %xmm7
	movss	7 * SIZE(X), %xmm8
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
	andps	 %xmm15, %xmm3
	andps	 %xmm15, %xmm4
	andps	 %xmm15, %xmm5
	andps	 %xmm15, %xmm6
	andps	 %xmm15, %xmm7
	andps	 %xmm15, %xmm8
#endif

	addq	$8 * SIZE, X

	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm4
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm5
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm6
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm7
	je	.L999
	incq	RET
	jmp	.L999
	ALIGN_4

.L25:
	testq	$4, M
	je	.L26

	movss	0 * SIZE(X), %xmm1
	movss	1 * SIZE(X), %xmm2
	movss	2 * SIZE(X), %xmm3
	movss	3 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
	andps	 %xmm15, %xmm3
	andps	 %xmm15, %xmm4
#endif
	addq	$4 * SIZE, X
	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L26:
	testq	$2, M
	je	.L27

	movss	0 * SIZE(X), %xmm1
	movss	1 * SIZE(X), %xmm2
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
#endif
	addq	$2 * SIZE, X
	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L27:
	incq	RET
	jmp	.L999
	ALIGN_3

/* Unaligned Mode */
.L30:
	movq	M,  I
	sarq	$4, I
	jle	.L35
	ALIGN_4

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	 0 * SIZE(X), %xmm4
	movhps	 2 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movsd	 4 * SIZE(X), %xmm5
	movhps	 6 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1

	movsd	 8 * SIZE(X), %xmm6
	movhps	10 * SIZE(X), %xmm6
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxps	%xmm6, %xmm2

	movsd	12 * SIZE(X), %xmm7
	movhps	14 * SIZE(X), %xmm7
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxps	%xmm7, %xmm3

	addq	$16 * SIZE, X
	decq	I
	jg	.L31
	ALIGN_4

.L35:
	andq	$15,  M
	jle	.L40

	testq	$8, M
	je	.L36

	movsd	0 * SIZE(X), %xmm4
	movhps	2 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movsd	4 * SIZE(X), %xmm5
	movhps	6 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1

	addq	$8 * SIZE, X
	ALIGN_3

.L36:
	testq	$4, M
	je	.L37

	movsd	0 * SIZE(X), %xmm6
	movhps	2 * SIZE(X), %xmm6
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxps	%xmm6, %xmm2
	addq	$4 * SIZE, X
	ALIGN_3

.L37:
	testq	$2, M
	je	.L38

	movsd	0 * SIZE(X), %xmm7
	unpcklps %xmm7, %xmm7
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxps	%xmm7, %xmm3
	addq	$2 * SIZE, X

.L38:
	testq	$1, M
	je	.L40

	movss	0 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0
	jmp	.L40
	ALIGN_4

.L40:
	movq	XX, X
	movq	MM, M

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

	movq	M,  I
	sarq	$3, I
	jle	.L45
	ALIGN_4

.L43:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(X), %xmm1
	movhps	2 * SIZE(X), %xmm1
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
#endif
	cmpeqps	%xmm0, %xmm1

	movsd	4 * SIZE(X), %xmm3
	movhps	6 * SIZE(X), %xmm3
#ifdef USE_ABS
	andps	 %xmm15, %xmm3
#endif
	cmpeqps	%xmm0, %xmm3

	orps	%xmm3, %xmm1
#ifndef C_SUN
	movmskps %xmm1, %r11
#else
	.long	0xd9500f4c
#endif
	testq	 $15, %r11
	jne	 .L44

	addq	$8 * SIZE, X
	addq	$8, RET
	decq	I
	jg	.L43
	jmp	.L45
	ALIGN_3

.L44:
	movss	0 * SIZE(X), %xmm1
	movss	1 * SIZE(X), %xmm2
	movss	2 * SIZE(X), %xmm3
	movss	3 * SIZE(X), %xmm4
	movss	4 * SIZE(X), %xmm5
	movss	5 * SIZE(X), %xmm6
	movss	6 * SIZE(X), %xmm7
	movss	7 * SIZE(X), %xmm8
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
	andps	 %xmm15, %xmm3
	andps	 %xmm15, %xmm4
	andps	 %xmm15, %xmm5
	andps	 %xmm15, %xmm6
	andps	 %xmm15, %xmm7
	andps	 %xmm15, %xmm8
#endif

	addq	$8 * SIZE, X

	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm4
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm5
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm6
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm7
	je	.L999
	incq	RET
	jmp	.L999
	ALIGN_4

.L45:
	testq	$4, M
	je	.L46

	movss	0 * SIZE(X), %xmm1
	movss	1 * SIZE(X), %xmm2
	movss	2 * SIZE(X), %xmm3
	movss	3 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
	andps	 %xmm15, %xmm3
	andps	 %xmm15, %xmm4
#endif
	addq	$4 * SIZE, X
	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L46:
	testq	$2, M
	je	.L47

	movss	0 * SIZE(X), %xmm1
	movss	1 * SIZE(X), %xmm2
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
#endif
	addq	$2 * SIZE, X
	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L47:
	incq	RET
	jmp	.L999
	ALIGN_3

.L80:
	movq	M,  I
	sarq	$3, I
	jle	.L85
	ALIGN_4

.L81:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxss	%xmm5, %xmm1

	movss	0 * SIZE(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxss	%xmm6, %xmm2

	movss	0 * SIZE(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxss	%xmm7, %xmm3

	movss	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxss	%xmm5, %xmm1

	movss	0 * SIZE(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxss	%xmm6, %xmm2

	movss	0 * SIZE(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxss	%xmm7, %xmm3

	decq	I
	jg	.L81
	ALIGN_4

.L85:
	andq	$7,  M
	jle	.L90

	testq	$4, M
	je	.L86

	movss	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxss	%xmm5, %xmm1

	movss	0 * SIZE(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxss	%xmm6, %xmm2

	movss	0 * SIZE(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxss	%xmm7, %xmm3
	ALIGN_3

.L86:
	testq	$2, M
	je	.L87

	movss	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxss	%xmm5, %xmm1
	ALIGN_3

.L87:
	testq	$1, M
	je	.L90

	movss	0 * SIZE(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxss	%xmm6, %xmm2
	ALIGN_4

.L90:
	movq	XX, X
	movq	MM, M

	maxss	%xmm1, %xmm0
	maxss	%xmm3, %xmm2
	maxss	%xmm2, %xmm0
	shufps	$0, %xmm0, %xmm0

	movq	M,  I
	sarq	$3, I
	jle	.L95
	ALIGN_4

.L93:
	movss	0 * SIZE(X), %xmm1
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
#endif
	cmpeqss	%xmm0, %xmm1

	movss	0 * SIZE(X), %xmm2
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm2
#endif
	cmpeqss	%xmm0, %xmm2

	movss	0 * SIZE(X), %xmm3
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm3
#endif
	cmpeqss	%xmm0, %xmm3

	movss	0 * SIZE(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	cmpeqss	%xmm0, %xmm4

	movss	0 * SIZE(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	cmpeqps	%xmm0, %xmm5

	movss	0 * SIZE(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	cmpeqss	%xmm0, %xmm6

	movss	0 * SIZE(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	cmpeqss	%xmm0, %xmm7

	movss	0 * SIZE(X), %xmm8
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm8
#endif
	cmpeqss	%xmm0, %xmm8

	orps	%xmm2, %xmm1
	orps	%xmm4, %xmm3
	orps	%xmm6, %xmm5
	orps	%xmm8, %xmm7
	orps	%xmm3, %xmm1
	orps	%xmm7, %xmm5
	orps	%xmm5, %xmm1

#ifndef C_SUN
	movmskps %xmm1, %r11
#else
	.long	0xd9500f4c
#endif
	testq	 $15, %r11
	jne	 .L94

	addq	$8, RET
	decq	I
	jg	.L93
	jmp	.L95
	ALIGN_3

.L94:
	subq	INCX, X
	movss	0 * SIZE(X), %xmm8
	subq	INCX, X
	movss	0 * SIZE(X), %xmm7
	subq	INCX, X
	movss	0 * SIZE(X), %xmm6
	subq	INCX, X
	movss	0 * SIZE(X), %xmm5
	subq	INCX, X
	movss	0 * SIZE(X), %xmm4
	subq	INCX, X
	movss	0 * SIZE(X), %xmm3
	subq	INCX, X
	movss	0 * SIZE(X), %xmm2
	subq	INCX, X
	movss	0 * SIZE(X), %xmm1
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
	andps	 %xmm15, %xmm3
	andps	 %xmm15, %xmm4
	andps	 %xmm15, %xmm5
	andps	 %xmm15, %xmm6
	andps	 %xmm15, %xmm7
	andps	 %xmm15, %xmm8
#endif
	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm4
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm5
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm6
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm7
	je	.L999
	incq	RET
	jmp	.L999
	ALIGN_4

.L95:
	testq	$4, M
	je	.L96

	movss	0 * SIZE(X), %xmm1
	addq	INCX, X
	movss	0 * SIZE(X), %xmm2
	addq	INCX, X
	movss	0 * SIZE(X), %xmm3
	addq	INCX, X
	movss	0 * SIZE(X), %xmm4
	addq	INCX, X

#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
	andps	 %xmm15, %xmm3
	andps	 %xmm15, %xmm4
#endif
	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm3
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm4
	je	.L999
	ALIGN_3

.L96:
	testq	$2, M
	je	.L97

	movss	0 * SIZE(X), %xmm1
	addq	INCX, X
	movss	0 * SIZE(X), %xmm2
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm1
	andps	 %xmm15, %xmm2
#endif
	incq	RET
	comiss	%xmm0, %xmm1
	je	.L999
	incq	RET
	comiss	%xmm0, %xmm2
	je	.L999
	ALIGN_3

.L97:
	incq	RET
	ALIGN_3

.L999:
	RESTOREREGISTERS

	ret

	EPILOGUE

