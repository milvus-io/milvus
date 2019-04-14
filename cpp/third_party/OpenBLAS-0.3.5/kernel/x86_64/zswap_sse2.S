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

#ifndef WINDOWS_ABI
#define M	ARG1	/* rdi */
#define X	ARG4
#define INCX	ARG5
#define Y	ARG6
#define INCY	ARG2
#else
#define M	ARG1
#define X	ARG2
#define INCX	ARG3
#define Y	ARG4
#define INCY	%rbx
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifndef WINDOWS_ABI
	movq	 8(%rsp), INCY
#else
	pushq	%rbx

	movq	56(%rsp), X
	movq	64(%rsp), INCX
	movq	72(%rsp), Y
	movq	80(%rsp), INCY
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	testq	M, M
	jle	.L19

	cmpq	$2 * SIZE, INCX
	jne	.L50
	cmpq	$2 * SIZE, INCY
	jne	.L50

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	testq	$SIZE, Y
	jne	.L30

	testq	$SIZE, X
	jne	.L20

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L13
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	movaps	-14 * SIZE(X), %xmm0
	movaps	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-14 * SIZE(Y)
	movaps	%xmm1, 	-14 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	-12 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-12 * SIZE(Y)
	movaps	%xmm1, 	-12 * SIZE(X)

	movaps	-10 * SIZE(X), %xmm0
	movaps	-10 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-10 * SIZE(Y)
	movaps	%xmm1, 	-10 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	 -8 * SIZE(X), %xmm0
	movaps	 -8 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -8 * SIZE(Y)
	movaps	%xmm1, 	 -8 * SIZE(X)

	movaps	 -6 * SIZE(X), %xmm0
	movaps	 -6 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -6 * SIZE(Y)
	movaps	%xmm1, 	 -6 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movaps	 -4 * SIZE(X), %xmm0
	movaps	 -4 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -4 * SIZE(Y)
	movaps	%xmm1, 	 -4 * SIZE(X)

	movaps	 -2 * SIZE(X), %xmm0
	movaps	 -2 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -2 * SIZE(Y)
	movaps	%xmm1, 	 -2 * SIZE(X)

	subq	$-16 * SIZE, Y
	subq	$-16 * SIZE, X

	decq	%rax
	jg	.L11
	ALIGN_3

.L13:
	testq	$4, M
	jle	.L14

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	movaps	-14 * SIZE(X), %xmm0
	movaps	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-14 * SIZE(Y)
	movaps	%xmm1, 	-14 * SIZE(X)

	movaps	-12 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-12 * SIZE(Y)
	movaps	%xmm1, 	-12 * SIZE(X)

	movaps	-10 * SIZE(X), %xmm0
	movaps	-10 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-10 * SIZE(Y)
 	movaps	%xmm1, 	-10 * SIZE(X)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L14:
	testq	$2, M
	jle	.L15

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	movaps	-14 * SIZE(X), %xmm0
	movaps	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-14 * SIZE(Y)
	movaps	%xmm1, 	-14 * SIZE(X)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L15:
	testq	$1, M
	jle	.L19

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L19:
	xorq	%rax,%rax

	RESTOREREGISTERS

#ifdef WINDOWS_ABI
	popq	%rbx
#endif
	ret
	ALIGN_3

.L20:
	movhps	 -16 * SIZE(X), %xmm0
	movaps	 -16 * SIZE(Y), %xmm1

	movlps	%xmm1, -16 * SIZE(X)
	decq	M
	jle	.L29

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L23
	ALIGN_4

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	-15 * SIZE(X), %xmm2
	movaps	-14 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(X)

	movaps	-13 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -14 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3, -13 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	-11 * SIZE(X), %xmm2
	movaps	-10 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -12 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -11 * SIZE(X)

	movaps	 -9 * SIZE(X), %xmm0
	movaps	 -8 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -10 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -9 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	 -7 * SIZE(X), %xmm2
	movaps	 -6 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1,  -7 * SIZE(X)

	movaps	 -5 * SIZE(X), %xmm0
	movaps	 -4 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2,  -6 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -5 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movaps	 -3 * SIZE(X), %xmm2
	movaps	 -2 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0,  -4 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1,  -3 * SIZE(X)

	movaps	 -1 * SIZE(X), %xmm0
	movaps	  0 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2,  -2 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -1 * SIZE(X)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	decq	%rax
	jg	.L21
	ALIGN_3

.L23:
	testq	$4, M
	jle	.L24

	movaps	-15 * SIZE(X), %xmm2
	movaps	-14 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(X)

	movaps	-13 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -14 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3, -13 * SIZE(X)

	movaps	-11 * SIZE(X), %xmm2
	movaps	-10 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -12 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -11 * SIZE(X)

	movaps	 -9 * SIZE(X), %xmm0
	movaps	 -8 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -10 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -9 * SIZE(X)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L24:
	testq	$2, M
	jle	.L25

	movaps	-15 * SIZE(X), %xmm2
	movaps	-14 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(X)

	movaps	-13 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -14 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3, -13 * SIZE(X)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L25:
	testq	$1, M
	jle	.L29

	movaps	-15 * SIZE(X), %xmm2
	movaps	-14 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(X)
	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L29:
	movaps	-15 * SIZE(X), %xmm2

	movhps	%xmm1, -15 * SIZE(X)
	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	xorq	%rax,%rax

	RESTOREREGISTERS

#ifdef WINDOWS_ABI
	popq	%rbx
#endif

	ret
	ALIGN_3

.L30:
	testq	$SIZE, X
	jne	.L40

	movhps	 -16 * SIZE(Y), %xmm0
	movaps	 -16 * SIZE(X), %xmm1

	movlps	%xmm1, -16 * SIZE(Y)
	decq	M
	jle	.L39

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L33
	ALIGN_4

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	-15 * SIZE(Y), %xmm2
	movaps	-14 * SIZE(X), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(X)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(Y)

	movaps	-13 * SIZE(Y), %xmm0
	movaps	-12 * SIZE(X), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -14 * SIZE(X)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3, -13 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	-11 * SIZE(Y), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -12 * SIZE(X)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -11 * SIZE(Y)

	movaps	 -9 * SIZE(Y), %xmm0
	movaps	 -8 * SIZE(X), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -10 * SIZE(X)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -9 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movaps	 -7 * SIZE(Y), %xmm2
	movaps	 -6 * SIZE(X), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0,  -8 * SIZE(X)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1,  -7 * SIZE(Y)

	movaps	 -5 * SIZE(Y), %xmm0
	movaps	 -4 * SIZE(X), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2,  -6 * SIZE(X)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -5 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	 -3 * SIZE(Y), %xmm2
	movaps	 -2 * SIZE(X), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0,  -4 * SIZE(X)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1,  -3 * SIZE(Y)

	movaps	 -1 * SIZE(Y), %xmm0
	movaps	  0 * SIZE(X), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2,  -2 * SIZE(X)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -1 * SIZE(Y)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	decq	%rax
	jg	.L31
	ALIGN_3

.L33:
	testq	$4, M
	jle	.L34

	movaps	-15 * SIZE(Y), %xmm2
	movaps	-14 * SIZE(X), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(X)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(Y)

	movaps	-13 * SIZE(Y), %xmm0
	movaps	-12 * SIZE(X), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -14 * SIZE(X)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3, -13 * SIZE(Y)

	movaps	-11 * SIZE(Y), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -12 * SIZE(X)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -11 * SIZE(Y)

	movaps	 -9 * SIZE(Y), %xmm0
	movaps	 -8 * SIZE(X), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -10 * SIZE(X)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -9 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L34:
	testq	$2, M
	jle	.L35

	movaps	-15 * SIZE(Y), %xmm2
	movaps	-14 * SIZE(X), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(X)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(Y)

	movaps	-13 * SIZE(Y), %xmm0
	movaps	-12 * SIZE(X), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -14 * SIZE(X)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3, -13 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L35:
	testq	$1, M
	jle	.L39

	movaps	-15 * SIZE(Y), %xmm2
	movaps	-14 * SIZE(X), %xmm3

	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(Y)
	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(X)

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L39:
	movaps	-15 * SIZE(Y), %xmm2

	movhps	%xmm1, -15 * SIZE(Y)
	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(X)

	xorq	%rax,%rax

	RESTOREREGISTERS

#ifdef WINDOWS_ABI
	popq	%rbx
#endif

	ret
	ALIGN_3

.L40:
	movsd	-16 * SIZE(X), %xmm0
	movsd	-16 * SIZE(Y), %xmm1

	movlps	%xmm0, 	-16 * SIZE(Y)
	movlps	%xmm1, 	-16 * SIZE(X)

	addq	$SIZE, X
	addq	$SIZE, Y
	decq	M
	jle	.L49

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L43
	ALIGN_3

.L41:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	movaps	-14 * SIZE(X), %xmm0
	movaps	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-14 * SIZE(Y)
	movaps	%xmm1, 	-14 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	-12 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-12 * SIZE(Y)
	movaps	%xmm1, 	-12 * SIZE(X)

	movaps	-10 * SIZE(X), %xmm0
	movaps	-10 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-10 * SIZE(Y)
	movaps	%xmm1, 	-10 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	 -8 * SIZE(X), %xmm0
	movaps	 -8 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -8 * SIZE(Y)
	movaps	%xmm1, 	 -8 * SIZE(X)

	movaps	 -6 * SIZE(X), %xmm0
	movaps	 -6 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -6 * SIZE(Y)
	movaps	%xmm1, 	 -6 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movaps	 -4 * SIZE(X), %xmm0
	movaps	 -4 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -4 * SIZE(Y)
	movaps	%xmm1, 	 -4 * SIZE(X)

	movaps	 -2 * SIZE(X), %xmm0
	movaps	 -2 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -2 * SIZE(Y)
	movaps	%xmm1, 	 -2 * SIZE(X)

	subq	$-16 * SIZE, Y
	subq	$-16 * SIZE, X

	decq	%rax
	jg	.L41
	ALIGN_3

.L43:
	testq	$4, M
	jle	.L44

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	movaps	-14 * SIZE(X), %xmm0
	movaps	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-14 * SIZE(Y)
	movaps	%xmm1, 	-14 * SIZE(X)

	movaps	-12 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-12 * SIZE(Y)
	movaps	%xmm1, 	-12 * SIZE(X)

	movaps	-10 * SIZE(X), %xmm0
	movaps	-10 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-10 * SIZE(Y)
 	movaps	%xmm1, 	-10 * SIZE(X)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L44:
	testq	$2, M
	jle	.L45

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	movaps	-14 * SIZE(X), %xmm0
	movaps	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-14 * SIZE(Y)
	movaps	%xmm1, 	-14 * SIZE(X)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L45:
	testq	$1, M
	jle	.L49

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L49:
	movsd	-16 * SIZE(X), %xmm0
	movsd	-16 * SIZE(Y), %xmm1

	movlps	%xmm0, 	-16 * SIZE(Y)
	movlps	%xmm1, 	-16 * SIZE(X)

	xorq	%rax,%rax

	RESTOREREGISTERS

#ifdef WINDOWS_ABI
	popq	%rbx
#endif
	ret
	ALIGN_3

.L50:
	testq	$SIZE, X
	jne	.L60
	testq	$SIZE, Y
	jne	.L60

	movq	M,  %rax
	sarq	$2, %rax
	jle	.L55
	ALIGN_3

.L51:
	movaps	(X), %xmm0
	movaps	(Y), %xmm1

	movaps	%xmm1, (X)
	addq	INCX, X
	movaps	%xmm0, (Y)
	addq	INCY, Y

	movaps	(X), %xmm0
	movaps	(Y), %xmm1

	movaps	%xmm1, (X)
	addq	INCX, X
	movaps	%xmm0, (Y)
	addq	INCY, Y

	movaps	(X), %xmm0
	movaps	(Y), %xmm1

	movaps	%xmm1, (X)
	addq	INCX, X
	movaps	%xmm0, (Y)
	addq	INCY, Y

	movaps	(X), %xmm0
	movaps	(Y), %xmm1

	movaps	%xmm1, (X)
	addq	INCX, X
	movaps	%xmm0, (Y)
	addq	INCY, Y

	decq	%rax
	jg	.L51
	ALIGN_3

.L55:
	movq	M,  %rax
	andq	$3, %rax
	jle	.L57
	ALIGN_3

.L56:
	movaps	(X), %xmm0
	movaps	(Y), %xmm1

	movaps	%xmm1, (X)
	addq	INCX, X
	movaps	%xmm0, (Y)
	addq	INCY, Y

	decq	%rax
	jg	.L56
	ALIGN_3

.L57:
	xorq	%rax, %rax

	RESTOREREGISTERS

#ifdef WINDOWS_ABI
	popq	%rbx
#endif

	ret
	ALIGN_3

.L60:
	movq	M,  %rax
	sarq	$2, %rax
	jle	.L65
	ALIGN_3

.L61:
	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0
	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1

	movlps	%xmm1, 0 * SIZE(X)
	movhps	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	movlps	%xmm0, 0 * SIZE(Y)
	movhps	%xmm0, 1 * SIZE(Y)
	addq	INCY, Y

	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0
	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1

	movlps	%xmm1, 0 * SIZE(X)
	movhps	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	movlps	%xmm0, 0 * SIZE(Y)
	movhps	%xmm0, 1 * SIZE(Y)
	addq	INCY, Y

	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0
	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1

	movlps	%xmm1, 0 * SIZE(X)
	movhps	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	movlps	%xmm0, 0 * SIZE(Y)
	movhps	%xmm0, 1 * SIZE(Y)
	addq	INCY, Y

	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0
	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1

	movlps	%xmm1, 0 * SIZE(X)
	movhps	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	movlps	%xmm0, 0 * SIZE(Y)
	movhps	%xmm0, 1 * SIZE(Y)
	addq	INCY, Y

	decq	%rax
	jg	.L61
	ALIGN_3

.L65:
	movq	M,  %rax
	andq	$3, %rax
	jle	.L67
	ALIGN_3

.L66:
	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0
	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1

	movlps	%xmm1, 0 * SIZE(X)
	movhps	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	movlps	%xmm0, 0 * SIZE(Y)
	movhps	%xmm0, 1 * SIZE(Y)
	addq	INCY, Y

	decq	%rax
	jg	.L66
	ALIGN_3

.L67:
	xorq	%rax, %rax

	RESTOREREGISTERS

#ifdef WINDOWS_ABI
	popq	%rbx
#endif

	ret

	EPILOGUE
