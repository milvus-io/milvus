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

	pxor	%xmm0, %xmm0
	salq	$ZBASE_SHIFT, INCX

	testq	M, M
	jle	.L999

	pcmpeqb	%xmm15, %xmm15
	psrld	$1, %xmm15

	movss	0 * SIZE(X), %xmm0
	movss	1 * SIZE(X), %xmm1
	addq	INCX, X
	decq	M
	andps	%xmm15, %xmm0
	andps	%xmm15, %xmm1
	addps	%xmm1,  %xmm0
	shufps	$0, %xmm0, %xmm0
	movaps	%xmm0, %xmm1
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
	movhps	2 * SIZE(X), %xmm4
	movsd	4 * SIZE(X), %xmm5
	movhps	6 * SIZE(X), %xmm5

	movaps	%xmm4, %xmm6

	shufps	$0x88, %xmm5, %xmm4
	shufps	$0xdd, %xmm5, %xmm6

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm6
	addps	%xmm6,  %xmm4
	maxps	%xmm4,  %xmm0

	movsd	 8 * SIZE(X), %xmm7
	movhps	10 * SIZE(X), %xmm7
	movsd	12 * SIZE(X), %xmm8
	movhps	14 * SIZE(X), %xmm8
	movaps	%xmm7, %xmm9

	shufps	$0x88, %xmm8, %xmm7
	shufps	$0xdd, %xmm8, %xmm9

	andps	%xmm15, %xmm7
	andps	%xmm15, %xmm9
	addps	%xmm9,  %xmm7
	maxps	%xmm7,  %xmm0

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
	movhps	2 * SIZE(X), %xmm4
	movsd	4 * SIZE(X), %xmm5
	movhps	6 * SIZE(X), %xmm5
	movaps	%xmm4, %xmm6

	shufps	$0x88, %xmm5, %xmm4
	shufps	$0xdd, %xmm5, %xmm6

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm6
	addps	%xmm6,  %xmm4
	maxps	%xmm4,  %xmm0

	addq	$8 * SIZE, X
	ALIGN_3

.L36:
	testq	$2, M
	je	.L37

	movss	0 * SIZE(X), %xmm4
	movss	1 * SIZE(X), %xmm5
	movss	2 * SIZE(X), %xmm6
	movss	3 * SIZE(X), %xmm7
	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5
	andps	%xmm15, %xmm6
	andps	%xmm15, %xmm7
	addps	%xmm5,  %xmm4
	addps	%xmm7,  %xmm6
	maxss	%xmm4,  %xmm0
	maxss	%xmm6,  %xmm1
	addq	$4 * SIZE, X
	ALIGN_3

.L37:
	testq	$1, M
	je	.L998

	movss	0 * SIZE(X), %xmm4
	movss	1 * SIZE(X), %xmm5
	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5
	addps	%xmm5,  %xmm4
	maxss	%xmm4,  %xmm0
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
	addq	INCX, X
	movhps	0 * SIZE(X), %xmm4
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm5
	addq	INCX, X
	movhps	0 * SIZE(X), %xmm5
	addq	INCX, X

	movaps	%xmm4, %xmm6

	shufps	$0x88, %xmm5, %xmm4
	shufps	$0xdd, %xmm5, %xmm6

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm6
	addps	%xmm6,  %xmm4
	maxps	%xmm4,  %xmm0

	movsd	 0 * SIZE(X), %xmm7
	addq	INCX, X
	movhps	 0 * SIZE(X), %xmm7
	addq	INCX, X
	movsd	 0 * SIZE(X), %xmm8
	addq	INCX, X
	movhps	 0 * SIZE(X), %xmm8
	addq	INCX, X
	movaps	%xmm7, %xmm9

	shufps	$0x88, %xmm8, %xmm7
	shufps	$0xdd, %xmm8, %xmm9

	andps	%xmm15, %xmm7
	andps	%xmm15, %xmm9
	addps	%xmm9,  %xmm7
	maxps	%xmm7,  %xmm0

	decq	I
	jg	.L41
	ALIGN_4

.L45:
	andq	$7,  M
	jle	.L998

	testq	$4, M
	je	.L46

	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
	movhps	0 * SIZE(X), %xmm4
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm5
	addq	INCX, X
	movhps	0 * SIZE(X), %xmm5
	addq	INCX, X
	movaps	%xmm4, %xmm6

	shufps	$0x88, %xmm5, %xmm4
	shufps	$0xdd, %xmm5, %xmm6

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm6
	addps	%xmm6,  %xmm4
	maxps	%xmm4,  %xmm0
	ALIGN_3

.L46:
	testq	$2, M
	je	.L47

	movss	0 * SIZE(X), %xmm4
	movss	1 * SIZE(X), %xmm5
	addq	INCX, X
	movss	0 * SIZE(X), %xmm6
	movss	1 * SIZE(X), %xmm7
	addq	INCX, X
	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5
	andps	%xmm15, %xmm6
	andps	%xmm15, %xmm7
	addps	%xmm5,  %xmm4
	addps	%xmm7,  %xmm6
	maxss	%xmm4,  %xmm0
	maxss	%xmm6,  %xmm1
	ALIGN_3

.L47:
	testq	$1, M
	je	.L998

	movss	0 * SIZE(X), %xmm4
	movss	1 * SIZE(X), %xmm5
	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5
	addps	%xmm5,  %xmm4
	maxss	%xmm4,  %xmm0
	jmp	.L998
	ALIGN_4

.L998:
	maxps	%xmm1, %xmm0
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
