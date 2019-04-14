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
#define maxps	minps
#define maxss	minss
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
	psrld	$1,    %xmm15
#endif

	movss	(X), %xmm0
	shufps	$0,  %xmm0, %xmm0
#ifdef USE_ABS
	andps	%xmm15, %xmm0
#endif
	movaps	%xmm0, %xmm1
	movaps	%xmm0, %xmm2
	movaps	%xmm0, %xmm3
	addq	INCX, X
	decq	M
	jle	.L999

	cmpq	$SIZE, INCX
	jne	.L40

	subq	$-32 * SIZE, X

	cmpq	$3, M
	jle	.L17

	testq	$SIZE, X
	je	.L05

	movss	-32 * SIZE(X), %xmm1
	shufps	$0, %xmm1, %xmm1
#ifdef USE_ABS
	andps	%xmm15, %xmm1
#endif
	decq	M
	addq	$SIZE, X
	ALIGN_3

.L05:
	testq	$2 * SIZE, X
	je	.L06

	movsd	-32 * SIZE(X), %xmm2
	unpcklps  %xmm2, %xmm2
#ifdef USE_ABS
	andps	%xmm15, %xmm2
#endif
	subq	$2, M
	addq	$2 * SIZE, X
	ALIGN_3

.L06:
	movq	M,  I
	sarq	$5, I
	jle	.L15

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-24 * SIZE(X), %xmm6
	movaps	-20 * SIZE(X), %xmm7

	decq	I
	jle	.L12
	ALIGN_4

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1
	movaps	-12 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxps	%xmm6, %xmm2
	movaps	 -8 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxps	%xmm7, %xmm3
	movaps	 -4 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1
	movaps	  4 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxps	%xmm6, %xmm2
	movaps	  8 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxps	%xmm7, %xmm3
	movaps	 12 * SIZE(X), %xmm7

	subq	$-32 * SIZE, X
	decq	I
	jg	.L11
	ALIGN_4

.L12:
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1
	movaps	-12 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxps	%xmm6, %xmm2
	movaps	 -8 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxps	%xmm7, %xmm3
	movaps	 -4 * SIZE(X), %xmm7

#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0

#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1

#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxps	%xmm6, %xmm2

#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxps	%xmm7, %xmm3

	subq	$-32 * SIZE, X
	ALIGN_3


.L15:
	testq	$16, M
	je	.L16

	movaps	-32 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movaps	-28 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1

	movaps	-24 * SIZE(X), %xmm6
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxps	%xmm6, %xmm2

	movaps	-20 * SIZE(X), %xmm7
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxps	%xmm7, %xmm3

	addq	$16 * SIZE, X
	ALIGN_3

.L16:
	testq	$8, M
	je	.L17

	movaps	-32 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movaps	-28 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxps	%xmm5, %xmm1
	addq	$8 * SIZE, X
	ALIGN_3

.L17:
	testq	$4, M
	je	.L18

	movaps	-32 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm2
	addq	$4 * SIZE, X
	ALIGN_3

.L18:
	testq	$2, M
	je	.L19

	movsd	-32 * SIZE(X), %xmm4
	unpcklps %xmm4, %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxps	%xmm4, %xmm3
	addq	$2 * SIZE, X
	ALIGN_3

.L19:
	testq	$1, M
	je	.L998

	movss	-32 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0
	jmp	.L998
	ALIGN_3

.L40:
	movq	M,  I
	sarq	$3, I
	jle	.L45
	ALIGN_4

.L41:
	movss	(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxss	%xmm5, %xmm1

	movss	(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxss	%xmm6, %xmm2

	movss	(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxss	%xmm7, %xmm3

	movss	(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxss	%xmm5, %xmm1

	movss	(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxss	%xmm6, %xmm2

	movss	(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxss	%xmm7, %xmm3

	decq	I
	jg	.L41
	ALIGN_4

.L45:
	testq	$4, M
	je	.L46

	movss	(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxss	%xmm5, %xmm1

	movss	(X), %xmm6
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm6
#endif
	maxss	%xmm6, %xmm2

	movss	(X), %xmm7
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm7
#endif
	maxss	%xmm7, %xmm3
	ALIGN_3

.L46:
	testq	$2, M
	je	.L47

	movss	(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	(X), %xmm5
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm5
#endif
	maxss	%xmm5, %xmm1
	ALIGN_3

.L47:
	testq	$1, M
	je	.L998

	movss	(X), %xmm4
	addq	INCX, X
#ifdef USE_ABS
	andps	%xmm15, %xmm4
#endif
	maxss	%xmm4, %xmm2
	ALIGN_4

.L998:
	maxps	%xmm1, %xmm0
	maxps	%xmm3, %xmm2
	maxps	%xmm2, %xmm0
	movaps	%xmm0, %xmm1
	movhlps %xmm0, %xmm0
	maxps	%xmm1, %xmm0
	movaps	%xmm0, %xmm1
	shufps	$1, %xmm0, %xmm0
	maxss	%xmm1, %xmm0
	ALIGN_4

.L999:
	RESTOREREGISTERS

	ret

	EPILOGUE
