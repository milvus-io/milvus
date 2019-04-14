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

#define I	%rax

#ifdef USE_MIN
#define maxpd	minpd
#define maxsd	minsd
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

	SAVEREGISTERS

	pxor	%xmm0, %xmm0
	testq	M, M
	jle	.L999
	testq	INCX, INCX
	jle	.L999

	salq	$ZBASE_SHIFT, INCX

	pcmpeqb	%xmm15, %xmm15
	psrlq	$1, %xmm15

	movsd	0 * SIZE(X), %xmm0
	movsd	1 * SIZE(X), %xmm1
	addq	INCX, X
	decq	M
	andpd	 %xmm15, %xmm0
	andpd	 %xmm15, %xmm1
	addpd	 %xmm1, %xmm0
	unpcklpd %xmm0, %xmm0
	movapd	 %xmm0, %xmm1
	movapd	 %xmm0, %xmm2
	movapd	 %xmm0, %xmm3

	cmpq	$2 * SIZE, INCX
	jne	.L40

.L30:
	movq	M,  I
	sarq	$3, I
	jle	.L35
	ALIGN_4

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	movhpd	2 * SIZE(X), %xmm4
	movhpd	3 * SIZE(X), %xmm5
	movsd	4 * SIZE(X), %xmm6
	movsd	5 * SIZE(X), %xmm7
	movhpd	6 * SIZE(X), %xmm6
	movhpd	7 * SIZE(X), %xmm7

	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	addpd	%xmm5,  %xmm4
	maxpd	%xmm4,  %xmm0

	andpd	%xmm15, %xmm6
	andpd	%xmm15, %xmm7
	addpd	%xmm7,  %xmm6
	maxpd	%xmm6,  %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movsd	 8 * SIZE(X), %xmm4
	movsd	 9 * SIZE(X), %xmm5
	movhpd	10 * SIZE(X), %xmm4
	movhpd	11 * SIZE(X), %xmm5
	movsd	12 * SIZE(X), %xmm6
	movsd	13 * SIZE(X), %xmm7
	movhpd	14 * SIZE(X), %xmm6
	movhpd	15 * SIZE(X), %xmm7

	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	addpd	%xmm5,  %xmm4
	maxpd	%xmm4,  %xmm2

	andpd	%xmm15, %xmm6
	andpd	%xmm15, %xmm7
	addpd	%xmm7,  %xmm6
	maxpd	%xmm6,  %xmm3

	addq	$16 * SIZE, X
	decq	I
	jg	.L31
	ALIGN_4

.L35:
	andq	$7,  M
	jle	.L998

	testq	$4, M
	je	.L36

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	movhpd	2 * SIZE(X), %xmm4
	movhpd	3 * SIZE(X), %xmm5
	movsd	4 * SIZE(X), %xmm6
	movsd	5 * SIZE(X), %xmm7
	movhpd	6 * SIZE(X), %xmm6
	movhpd	7 * SIZE(X), %xmm7

	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	andpd	%xmm15, %xmm6
	andpd	%xmm15, %xmm7

	addpd	%xmm5,  %xmm4
	addpd	%xmm7,  %xmm6
	maxpd	%xmm4, %xmm0
	maxpd	%xmm6, %xmm1

	addq	$8 * SIZE, X
	ALIGN_3

.L36:
	testq	$2, M
	je	.L37

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	movhpd	2 * SIZE(X), %xmm4
	movhpd	3 * SIZE(X), %xmm5
	addq	$4 * SIZE, X

	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	addpd	%xmm5,  %xmm4
	maxpd	%xmm4, %xmm0
	ALIGN_3

.L37:
	testq	$1, M
	je	.L998

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	addpd	%xmm5, %xmm4
	maxsd	%xmm4, %xmm2
	jmp	.L998
	ALIGN_4


.L40:
	movq	M,  I
	sarq	$3, I
	jle	.L45
	ALIGN_4

.L41:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm4
	movhpd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm6
	movsd	1 * SIZE(X), %xmm7
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm6
	movhpd	1 * SIZE(X), %xmm7
	addq	INCX, X

	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	addpd	%xmm5,  %xmm4
	maxpd	%xmm4,  %xmm0

	andpd	%xmm15, %xmm6
	andpd	%xmm15, %xmm7
	addpd	%xmm7,  %xmm6
	maxpd	%xmm6,  %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm4
	movhpd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm6
	movsd	1 * SIZE(X), %xmm7
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm6
	movhpd	1 * SIZE(X), %xmm7
	addq	INCX, X

	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	addpd	%xmm5,  %xmm4
	maxpd	%xmm4,  %xmm2

	andpd	%xmm15, %xmm6
	andpd	%xmm15, %xmm7
	addpd	%xmm7,  %xmm6
	maxpd	%xmm6,  %xmm3

	decq	I
	jg	.L41
	ALIGN_4

.L45:
	andq	$7,  M
	jle	.L998

	testq	$4, M
	je	.L46

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm4
	movhpd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm6
	movsd	1 * SIZE(X), %xmm7
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm6
	movhpd	1 * SIZE(X), %xmm7
	addq	INCX, X

	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	andpd	%xmm15, %xmm6
	andpd	%xmm15, %xmm7
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
	maxpd	%xmm4, %xmm0
	maxpd	%xmm6, %xmm1
	ALIGN_3

.L46:
	testq	$2, M
	je	.L47

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm4
	movhpd	1 * SIZE(X), %xmm5
	addq	INCX, X
	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	addpd	%xmm5, %xmm4
	maxpd	%xmm4, %xmm2
	ALIGN_3

.L47:
	testq	$1, M
	je	.L998

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	andpd	%xmm15, %xmm4
	andpd	%xmm15, %xmm5
	addpd	%xmm5, %xmm4
	maxsd	%xmm4, %xmm3
	jmp	.L998
	ALIGN_4

.L998:
	maxpd	%xmm1, %xmm0
	maxpd	%xmm3, %xmm2
	maxpd	%xmm2, %xmm0
	movapd	%xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	%xmm1, %xmm0
	ALIGN_4

.L999:
	RESTOREREGISTERS

	ret

	EPILOGUE
