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

	xorps	%xmm0, %xmm0
	leaq	(, INCX, SIZE), INCX

	testq	M, M
	jle	.L999

#ifdef USE_ABS
	pcmpeqb	%xmm15, %xmm15
	psrlq	$1, %xmm15
#endif

	movsd	(X), %xmm0
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm0
#endif
	unpcklpd %xmm0, %xmm0
	movaps	 %xmm0, %xmm1
	movaps	 %xmm0, %xmm2
	movaps	 %xmm0, %xmm3
	decq	M
	jle	.L999

	cmpq	$SIZE, INCX
	jne	.L40

	subq	$-16 * SIZE, X

	testq	$SIZE, X
	je	.L05

	movsd	-16 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	unpcklpd  %xmm4, %xmm4
	maxpd	  %xmm4, %xmm3
	addq	$SIZE, X
	decq	M
	jle	.L998
	ALIGN_3

.L05:
	movq	M,  I
	sarq	$4, I
	jle	.L15

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7

	decq	I
	jle	.L12
	ALIGN_4

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	movaps	 -6 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2
	movaps	 -4 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3
	movaps	 -2 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	movaps	  2 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2
	movaps	  4 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3
	movaps	  6 * SIZE(X), %xmm7

	subq	$-16 * SIZE, X
	decq	I
	jg	.L11
	ALIGN_4

.L12:
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	movaps	 -6 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2
	movaps	 -4 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3
	movaps	 -2 * SIZE(X), %xmm7

#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

	subq	$-16 * SIZE, X
	ALIGN_4

.L15:
	testq	$8, M
	jle	.L16

	movaps	-16 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movaps	-14 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movaps	-12 * SIZE(X), %xmm6
#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movaps	-10 * SIZE(X), %xmm7
#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3
	addq	$8 * SIZE, X
	ALIGN_3

.L16:
	testq	$4, M
	jle	.L17

	movaps	-16 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movaps	-14 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	addq	$4 * SIZE, X
	ALIGN_3

.L17:
	testq	$2, M
	jle	.L18

	movaps	-16 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm2
	addq	$2 * SIZE, X
	ALIGN_3

.L18:
	testq	$1, M
	jle	.L998

	movsd	-16 * SIZE(X), %xmm4
	unpcklpd  %xmm4, %xmm4
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm3
	jmp	.L998
	ALIGN_3

.L40:
	movq	M,  I
	sarq	$4, I
	jle	.L45
	ALIGN_4

.L41:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	(X), %xmm6
	addq	INCX, X
	movhps	(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movsd	(X), %xmm7
	addq	INCX, X
	movhps	(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	(X), %xmm6
	addq	INCX, X
	movhps	(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movsd	(X), %xmm7
	addq	INCX, X
	movhps	(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3

	decq	I
	jg	.L41
	ALIGN_4

.L45:
	andq	$15,  M
	jle	.L998

	testq	$8, M
	je	.L46

	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1

	movsd	(X), %xmm6
	addq	INCX, X
	movhps	(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2

	movsd	(X), %xmm7
	addq	INCX, X
	movhps	(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3
	ALIGN_3

.L46:
	testq	$4, M
	je	.L47

	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm4
#endif
	maxpd	%xmm4, %xmm0

	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm5
#endif
	maxpd	%xmm5, %xmm1
	ALIGN_3

.L47:
	testq	$2, M
	je	.L48

	movsd	(X), %xmm6
	addq	INCX, X
	movhps	(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	 %xmm15, %xmm6
#endif
	maxpd	%xmm6, %xmm2
	ALIGN_3

.L48:
	testq	$1, M
	je	.L998

	movsd	(X), %xmm7
	unpcklpd  %xmm7, %xmm7
#ifdef USE_ABS
	andps	 %xmm15, %xmm7
#endif
	maxpd	%xmm7, %xmm3
	ALIGN_4

.L998:
	maxpd	%xmm1, %xmm0
	maxpd	%xmm3, %xmm2
	maxpd	%xmm2, %xmm0
	movaps	%xmm0, %xmm1
	unpckhpd %xmm0, %xmm0
	maxsd	%xmm1, %xmm0
	ALIGN_4

.L999:
	RESTOREREGISTERS

	ret

	EPILOGUE
